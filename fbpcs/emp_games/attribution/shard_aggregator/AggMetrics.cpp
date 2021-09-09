/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "fbpcs/emp_games/attribution/shard_aggregator/AggMetrics.h"

#include <map>

#include <emp-sh2pc/emp-sh2pc.h>

#include <folly/dynamic.h>
#include <folly/logging/xlog.h>

namespace private_measurement {
AggMetricsTag AggMetrics::getTag() const {
  return tag_;
}

AggMetrics::MetricsValue AggMetrics::getValue() const {
  return value_;
}

AggMetrics::MetricsInt AggMetrics::getIntValue() const {
  checkMyType(AggMetricsTag::Integer);
  return std::get<AggMetrics::MetricsInt>(value_);
}

emp::Integer AggMetrics::getEmpIntValue() const {
  checkMyType(AggMetricsTag::EmpInteger);
  return std::get<emp::Integer>(value_);
}

void AggMetrics::setEmpIntValue(emp::Integer value) {
  checkMyType(AggMetricsTag::EmpInteger);
  value_ = std::move(value);
}

// list accessors/mutators
const AggMetrics::MetricsList& AggMetrics::getAsList() const {
  checkMyType(AggMetricsTag::List);
  return std::get<MetricsList>(value_);
}

std::shared_ptr<AggMetrics> AggMetrics::getAtIndex(std::size_t i) const {
  return getAsList().at(i);
}

void AggMetrics::pushBack(std::shared_ptr<AggMetrics> value) {
  checkMyType(AggMetricsTag::List);
  std::get<MetricsList>(value_).push_back(value);
}

// map accessors/mutators
const AggMetrics::MetricsMap& AggMetrics::getAsMap() const {
  checkMyType(AggMetricsTag::Map);
  return std::get<MetricsMap>(value_);
}

std::shared_ptr<AggMetrics> AggMetrics::getAtKey(const std::string& key) const {
  return getAsMap().at(key);
}

void AggMetrics::emplace(std::string key, std::shared_ptr<AggMetrics> value) {
  checkMyType(AggMetricsTag::Map);
  std::get<AggMetrics::MetricsMap>(value_).emplace(key, value);
}

AggMetrics AggMetrics::fromDynamic(const folly::dynamic& obj) {
  switch (obj.type()) {
    case folly::dynamic::OBJECT: {
      AggMetrics metrics = AggMetrics{AggMetricsTag::Map};
      for (const auto& [key, aggMetrics] : obj.items()) {
        metrics.emplace(
            key.asString(),
            std::make_shared<AggMetrics>(fromDynamic(aggMetrics)));
      }
      return metrics;
    }
    case folly::dynamic::ARRAY: {
      AggMetrics metrics = AggMetrics{AggMetricsTag::List};
      for (const auto& aggMetrics : obj) {
        metrics.pushBack(std::make_shared<AggMetrics>(fromDynamic(aggMetrics)));
      }
      return metrics;
    }
    case folly::dynamic::INT64:
      return AggMetrics{obj.asInt()};

    default:
      XLOG(FATAL) << "Metric values should be integers";
  }
}

folly::dynamic AggMetrics::toDynamic() const {
  switch (tag_) {
    case AggMetricsTag::Map: {
      folly::dynamic container = folly::dynamic::object();
      for (const auto& [key, value] : getAsMap()) {
        container.insert(key, value->toDynamic());
      }
      return container;
    }
    case AggMetricsTag::List: {
      auto container = folly::dynamic::array();
      std::transform(
          getAsList().begin(),
          getAsList().end(),
          std::back_inserter(container),
          [](auto m) { return m->toDynamic(); });
      return container;
    }
    case AggMetricsTag::Integer: {
      return getIntValue();
    }
    default:
      XLOG(FATAL) << "Metric values should be maps, lists, or integers here";
  }
}

void AggMetrics::printSpaces(std::ostream& os, int32_t n) const {
  for (auto i = 0; i < n; ++i) {
    os << ' ';
  }
}

void AggMetrics::print(std::ostream& os, int32_t tabbing) const {
  printSpaces(os, tabbing);
  switch (getTag()) {
    case AggMetricsTag::Map: {
      os << "map{\n";
      tabbing += 2;
      for (const auto& [key, inner] : getAsMap()) {
        printSpaces(os, tabbing);
        os << key << ":\n";
        inner->print(os, tabbing);
      }
      tabbing -= 2;
      printSpaces(os, tabbing);
      os << "}\n";
      break;
    }
    case AggMetricsTag::List: {
      os << "list[\n";
      tabbing += 2;
      for (const auto& inner : getAsList()) {
        inner->print(os, tabbing);
      }
      tabbing -= 2;
      printSpaces(os, tabbing);
      os << "]\n";
      break;
    }
    case AggMetricsTag::Integer: {
      os << '<' << getIntValue() << ">\n";
      break;
    }
    case AggMetricsTag::EmpInteger: {
      os << "<SECRET>\n";
      break;
    }
    default: {
      XLOG(FATAL) << "metrics contains unsupported tag";
      break;
    }
  }
}

std::ostream& operator<<(
    std::ostream& os,
    const private_measurement::AggMetrics& metrics) {
  metrics.print(os);
  return os;
}

std::shared_ptr<AggMetrics> AggMetrics::copy(
    const std::shared_ptr<AggMetrics>& metrics) {
  switch (metrics->getTag()) {
    case AggMetricsTag::Map: {
      AggMetrics copy{AggMetricsTag::Map};
      for (const auto& [key, innerMetrics] : metrics->getAsMap()) {
        copy.emplace(key, AggMetrics::copy(innerMetrics));
      }
      return std::make_shared<AggMetrics>(copy);
    }
    case AggMetricsTag::List: {
      AggMetrics copy{AggMetricsTag::List};
      for (const auto& innerMetrics : metrics->getAsList()) {
        copy.pushBack(AggMetrics::copy(innerMetrics));
      }
      return std::make_shared<AggMetrics>(copy);
    }
    case AggMetricsTag::EmpInteger: {
      return std::make_shared<AggMetrics>(
          AggMetrics{metrics->getEmpIntValue()});
    }
    default:
      return std::make_shared<AggMetrics>(AggMetrics{metrics->getIntValue()});
  }
}

// merges this AggMetrics structure with another one.  Merges matching map
// keys and list indexes, adding new keys/list entries if needed.
// Inner values should be emp::Integers and are merged via addition.
//
// Ex: Merging    metrics1 =  {[{"a": 1}, {"b": 5}]} with
//                metrics2 =  {[{"a": 2}, {"b": 3}]}
//     results in metrics1 <- {[{"a": 3}. {"b": 8}]}
void AggMetrics::mergeWithViaAddition(
    const std::shared_ptr<AggMetrics>& metrics) {
  checkMyType(metrics->getTag());

  switch (metrics->getTag()) {
    case AggMetricsTag::Map: {
      for (const auto& [key, innerMetrics] : metrics->getAsMap()) {
        // have not merged with this key yet
        if (getAsMap().find(key) == getAsMap().end()) {
          if (innerMetrics->getTag() == AggMetricsTag::EmpInteger) {
            emplace(
                key,
                std::make_shared<AggMetrics>(
                    AggMetrics{innerMetrics->getEmpIntValue()}));

          } else {
            // emplace the new structure and merge the correct values in
            emplace(
                key,
                std::make_shared<AggMetrics>(
                    AggMetrics{innerMetrics->getTag()}));
            getAtKey(key)->mergeWithViaAddition(innerMetrics);
          }

        } else {
          getAtKey(key)->mergeWithViaAddition(innerMetrics);
        }
      }
      break;
    }
    case AggMetricsTag::List: {
      for (std::size_t i = 0; i < metrics->getAsList().size(); ++i) {
        // have not merged with this list index yet
        if (getAsList().size() <= i) {
          if (metrics->getAtIndex(i)->getTag() == AggMetricsTag::EmpInteger) {
            pushBack(std::make_shared<AggMetrics>(
                AggMetrics{metrics->getAtIndex(i)->getEmpIntValue()}));

          } else {
            // push back the new structure and merge the correct values in
            pushBack(std::make_shared<AggMetrics>(
                AggMetrics{metrics->getAtIndex(i)->getTag()}));
            getAtIndex(i)->mergeWithViaAddition(metrics->getAtIndex(i));
          }
        } else {
          getAtIndex(i)->mergeWithViaAddition(metrics->getAtIndex(i));
        }
      }
      break;
    }
    case AggMetricsTag::EmpInteger: {
      // merge innermost values via addition
      value_ = getEmpIntValue() + metrics->getEmpIntValue();
      break;
    }
    default: {
      XLOG(FATAL)
          << "accumulator should only store a map, list, or emp::Integer at this point";
      break;
    }
  }
}

void AggMetrics::checkMyType(AggMetricsTag tag) const {
  if (tag_ != tag) {
    const std::map<AggMetricsTag, std::string> tagsMap = {
        {AggMetricsTag::Integer, "Integer"},
        {AggMetricsTag::EmpInteger, "EmpInteger"},
        {AggMetricsTag::List, "List"},
        {AggMetricsTag::Map, "Map"},
    };

    XLOG(FATAL) << "AggMetrics is of type " << tagsMap.at(tag_) << ", not "
                << tagsMap.at(tag);
  }
}
} // namespace private_measurement
