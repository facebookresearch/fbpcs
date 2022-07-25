/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <memory>
#include <ostream>
#include <queue>
#include <utility>

#include <folly/FBString.h>
#include <folly/Format.h>
#include <folly/dynamic.h>
#include <folly/json.h>

#include <fbpcf/exception/exceptions.h>
#include <fbpcf/io/FileManagerUtil.h>

#include <fbpcs/emp_games/common/Constants.h>
#include <fbpcs/emp_games/pcf2_shard_combiner/AggMetrics.h>

namespace shard_combiner {
template <
    int schedulerId,
    bool usingBatch,
    common::InputEncryption inputEncryption>
using AggMetric_t = AggMetrics<schedulerId, usingBatch, inputEncryption>;

template <
    int schedulerId,
    bool usingBatch,
    common::InputEncryption inputEncryption>
void AggMetrics<schedulerId, usingBatch, inputEncryption>::accumulateFinal(
    std::shared_ptr<AggMetrics<schedulerId, usingBatch, inputEncryption>>& lhs,
    const std::shared_ptr<AggMetrics<schedulerId, usingBatch, inputEncryption>>&
        rhs) {
  if constexpr (inputEncryption == common::InputEncryption::Plaintext) {
    lhs->setValue(lhs->getValue() + rhs->getValue());
  } else if constexpr (inputEncryption == common::InputEncryption::Xor) {
    auto res = lhs->getSecValueXor() + rhs->getSecValueXor();
    lhs->setSecValueXor(res);
  } else {
    throw common::exceptions::NotImplementedError(
        "This method will patched with tests in the future.");
  }
}

template <
    int schedulerId,
    bool usingBatch,
    common::InputEncryption inputEncryption>
void AggMetrics<schedulerId, usingBatch, inputEncryption>::accumulate(
    std::shared_ptr<AggMetrics<schedulerId, usingBatch, inputEncryption>>& lhs,
    const std::shared_ptr<AggMetrics<schedulerId, usingBatch, inputEncryption>>&
        rhs) {
  if (lhs->getType() != rhs->getType()) {
    throw common::exceptions::InvalidAccessError(
        "Rhs and lhs has to be of the same type");
  }

  using AggMetric_sp =
      std::shared_ptr<AggMetrics<schedulerId, usingBatch, inputEncryption>>;
  std::queue<std::pair<AggMetric_sp, AggMetric_sp>> q_;

  q_.push(std::make_pair(lhs, rhs));

  while (!q_.empty()) {
    auto [aggMetric, metric] = q_.front();
    q_.pop();
    switch (metric->getType()) {
      case AggMetricType::kDict: {
        auto aggMetricMap = aggMetric->getAsDict();
        for (const auto& [key, innerMetrics] : metric->getAsDict()) {
          auto innerAggMetrics = aggMetricMap.at(key);
          q_.push(make_pair(innerAggMetrics, innerMetrics));
        }
        break;
      }
      case AggMetricType::kList: {
        auto aggMetricList = aggMetric->getAsList();
        auto metricList = metric->getAsList();

        if (aggMetricList.size() != metricList.size()) {
          XLOG(ERR) << "Rhs and Lhs list do not match in size";
          throw common::exceptions::SchemaTraceError(
              "Rhs and Lhs list do not match in size");
        }
        for (size_t i = 0; i != aggMetricList.size(); ++i) {
          q_.push(std::make_pair(aggMetricList.at(i), metricList.at(i)));
        }
        break;
      }
      case AggMetricType::kValue: {
        accumulateFinal(aggMetric, metric);
        break;
      }
    }
  }
}

template <
    int schedulerId,
    bool usingBatch,
    common::InputEncryption inputEncryption>
typename AggMetrics<schedulerId, usingBatch, inputEncryption>::MetricsValue
AggMetrics<schedulerId, usingBatch, inputEncryption>::getValue() const {
  return std::get<MetricsValue>(val_);
}

template <
    int schedulerId,
    bool usingBatch,
    common::InputEncryption inputEncryption>
const typename AggMetrics<schedulerId, usingBatch, inputEncryption>::
    MetricsList&
    AggMetrics<schedulerId, usingBatch, inputEncryption>::getAsList() const {
  return std::get<MetricsList>(val_);
}

template <
    int schedulerId,
    bool usingBatch,
    common::InputEncryption inputEncryption>
const typename AggMetrics<schedulerId, usingBatch, inputEncryption>::
    MetricsDict&
    AggMetrics<schedulerId, usingBatch, inputEncryption>::getAsDict() const {
  return std::get<MetricsDict>(val_);
}

template <
    int schedulerId,
    bool usingBatch,
    common::InputEncryption inputEncryption>
std::shared_ptr<AggMetrics<schedulerId, usingBatch, inputEncryption>>
AggMetrics<schedulerId, usingBatch, inputEncryption>::getAtKey(
    std::string key) const {
  return std::get<MetricsDict>(val_).at(key);
}

template <
    int schedulerId,
    bool usingBatch,
    common::InputEncryption inputEncryption>
std::shared_ptr<AggMetrics<schedulerId, usingBatch, inputEncryption>>
AggMetrics<schedulerId, usingBatch, inputEncryption>::getAtIndex(
    size_t i) const {
  return std::get<MetricsList>(val_).at(i);
}

template <
    int schedulerId,
    bool usingBatch,
    common::InputEncryption inputEncryption>
void AggMetrics<schedulerId, usingBatch, inputEncryption>::setValue(
    AggMetrics<schedulerId, usingBatch, inputEncryption>::MetricsValue v) {
  this->val_ = v;
}

template <
    int schedulerId,
    bool usingBatch,
    common::InputEncryption inputEncryption>
void AggMetrics<schedulerId, usingBatch, inputEncryption>::
    updateSecValueFromRawInt() {
  if constexpr (inputEncryption == common::InputEncryption::Xor) {
    if constexpr (usingBatch) {
      std::vector<int64_t> val{getValue()};
      typename SecInt<schedulerId, usingBatch>::ExtractedInt extractedInt(val);
      SecInt<schedulerId, usingBatch> secInt =
          SecInt<schedulerId, usingBatch>(std::move(extractedInt));
      this->setSecValueXor(secInt);
    } else {
      auto errStr = folly::sformat(
          "Only batch mode is supported, got: usingBatch = {}", usingBatch);
      throw common::exceptions::NotImplementedError(
          "Only batch mode is supported currently.");
    }
  } else if constexpr (inputEncryption == common::InputEncryption::Plaintext) {
    // if plaintext do nothing.
  } else {
    auto errStr = folly::sformat(
        "Encryption type({}) is not supported.", (int)inputEncryption);
    XLOG(ERR, errStr);
    throw common::exceptions::NotImplementedError(errStr);
  }
}

template <
    int schedulerId,
    bool usingBatch,
    common::InputEncryption inputEncryption>
void AggMetrics<schedulerId, usingBatch, inputEncryption>::setList(
    AggMetrics<schedulerId, usingBatch, inputEncryption>::MetricsList& v) {
  this->val_ = std::move(v);
}

template <
    int schedulerId,
    bool usingBatch,
    common::InputEncryption inputEncryption>
void AggMetrics<schedulerId, usingBatch, inputEncryption>::setDict(
    AggMetrics<schedulerId, usingBatch, inputEncryption>::MetricsDict& v) {
  this->val_ = std::move(v);
}

template <
    int schedulerId,
    bool usingBatch,
    common::InputEncryption inputEncryption>
void AggMetrics<schedulerId, usingBatch, inputEncryption>::insert(
    std::pair<
        std::string,
        std::shared_ptr<AggMetrics<schedulerId, usingBatch, inputEncryption>>>
        kv) {
  if (type_ != AggMetricType::kDict) {
    XLOG(ERR) << "Incorrect operation for the type of member";
    throw common::exceptions::InvalidAccessError(
        "Incorrect operation on the metric type. Metric type should be a Dict.");
  }
  std::get<MetricsDict>(val_).insert(kv);
}

template <
    int schedulerId,
    bool usingBatch,
    common::InputEncryption inputEncryption>
void AggMetrics<schedulerId, usingBatch, inputEncryption>::pushBack(
    std::shared_ptr<AggMetrics<schedulerId, usingBatch, inputEncryption>>& v) {
  if (type_ != AggMetricType::kList) {
    XLOG(ERR) << "Incorrect operation for the type of member";
    throw common::exceptions::InvalidAccessError(
        "Incorrect operation on the metric type. Metric type should be a List.");
  }
  std::get<MetricsList>(val_).push_back(v);
}

template <
    int schedulerId,
    bool usingBatch,
    common::InputEncryption inputEncryption>
void AggMetrics<schedulerId, usingBatch, inputEncryption>::appendAtKey(
    std::string,
    std::shared_ptr<AggMetrics<schedulerId, usingBatch, inputEncryption>>
    /*v*/) {
  throw common::exceptions::NotImplementedError(
      "This method needs to be implemented");
}

template <
    int schedulerId,
    bool usingBatch,
    common::InputEncryption inputEncryption>
std::shared_ptr<AggMetrics<schedulerId, usingBatch, inputEncryption>>
AggMetrics<schedulerId, usingBatch, inputEncryption>::newLike(
    const std::shared_ptr<AggMetrics<schedulerId, usingBatch, inputEncryption>>&
        rhs) {
  using AggMetric_sp =
      std::shared_ptr<AggMetrics<schedulerId, usingBatch, inputEncryption>>;
  std::queue<std::pair<AggMetric_sp, AggMetric_sp>> q_; // pair of src, dst.

  std::shared_ptr<AggMetrics<schedulerId, usingBatch, inputEncryption>> retObj =
      std::make_shared<AggMetrics<schedulerId, usingBatch, inputEncryption>>(
          rhs->getType());
  q_.push(std::make_pair(retObj, rhs));

  while (!q_.empty()) {
    auto [dst, src] = q_.front();
    q_.pop();

    switch (src->getType()) {
      case AggMetricType::kList: {
        for (const auto& srcMetric : src->getAsList()) {
          auto newObj = std::make_shared<
              AggMetrics<schedulerId, usingBatch, inputEncryption>>(
              srcMetric->getType());
          dst->pushBack(newObj);
          q_.push(std::make_pair(newObj, srcMetric));
        }
        break;
      }
      case AggMetricType::kDict: {
        for (const auto& [k, srcMetric] : src->getAsDict()) {
          auto newObj = std::make_shared<
              AggMetrics<schedulerId, usingBatch, inputEncryption>>(
              srcMetric->getType());
          dst->insert(std::make_pair(k, newObj));
          q_.push(make_pair(newObj, srcMetric));
        }
        break;
      }
      case AggMetricType::kValue: {
        dst->setValue(0);
        if constexpr (inputEncryption == common::InputEncryption::Xor) {
          dst->setSecValXor(SecInt<schedulerId, usingBatch>(0));
        }
        break;
      }
    }
  }
  return retObj;
}

template <
    int schedulerId,
    bool usingBatch,
    common::InputEncryption inputEncryption>
std::shared_ptr<AggMetrics<schedulerId, usingBatch, inputEncryption>>
AggMetrics<schedulerId, usingBatch, inputEncryption>::fromJson(
    std::string filePath) {
  auto dynObj = folly::parseJson(fbpcf::io::read(filePath));

  using AggMetric_sp =
      std::shared_ptr<AggMetrics<schedulerId, usingBatch, inputEncryption>>;
  std::queue<std::pair<folly::dynamic, AggMetric_sp>> q_; // pair of src, dst.

  std::shared_ptr<AggMetrics<schedulerId, usingBatch, inputEncryption>> retObj;

  auto getAggObjByType = [](const folly::dynamic& obj)
      -> std::shared_ptr<AggMetrics<schedulerId, usingBatch, inputEncryption>> {
    if (obj.type() == folly::dynamic::INT64) {
      return std::make_shared<
          AggMetrics<schedulerId, usingBatch, inputEncryption>>(
          AggMetricType::kValue);
    } else if (obj.type() == folly::dynamic::ARRAY) {
      return std::make_shared<
          AggMetrics<schedulerId, usingBatch, inputEncryption>>(
          AggMetricType::kList);
    } else if (obj.type() == folly::dynamic::OBJECT) {
      return std::make_shared<
          AggMetrics<schedulerId, usingBatch, inputEncryption>>(
          AggMetricType::kDict);
    } else {
      XLOG(ERR)
          << "Parse error for type: " << obj.type()
          << ". We only support INT64, ARRAY and OBJECT from folly::dynamic class.";
      throw common::exceptions::NotImplementedError(
          "We only support INT64, ARRAY and OBJECT from folly::dynamic class.");
    }
  };

  retObj = getAggObjByType(dynObj);

  q_.push(std::make_pair(dynObj, retObj));

  /*
   * The loop below does a bfs over folly::dynamic objects src objects, and
   * creates corresponding AggMetrics graph.
   */
  while (!q_.empty()) {
    auto [src, dst] = q_.front();
    q_.pop();

    switch (src.type()) {
      case folly::dynamic::ARRAY: {
        for (const auto& srcDynMetric : src) {
          auto dstMetric = getAggObjByType(srcDynMetric);
          dst->pushBack(dstMetric);
          q_.push(std::make_pair(srcDynMetric, dstMetric));
        }
        break;
      }
      case folly::dynamic::OBJECT: {
        for (const auto& [k, srcDynMetric] : src.items()) {
          std::shared_ptr<AggMetrics<schedulerId, usingBatch, inputEncryption>>
              dstMetric = getAggObjByType(srcDynMetric);

          dst->insert(std::make_pair(k.asString(), dstMetric));
          q_.push(std::make_pair(srcDynMetric, dstMetric));
        }
        break;
      }
      case folly::dynamic::INT64: {
        dst->setValue(src.asInt());
        dst->updateSecValueFromRawInt();
        break;
      }
      default: {
        XLOG(ERR) << "Folly type not supported: " << src.type();
        throw common::exceptions::NotImplementedError(
            "Folly type is not supported ");
        break;
      }
    }
  }
  return retObj;
}

template <
    int schedulerId,
    bool usingBatch,
    common::InputEncryption inputEncryption>
void AggMetrics<schedulerId, usingBatch, inputEncryption>::printSpaces(
    std::ostream& os,
    int32_t n) const {
  for (auto i = 0; i < n; ++i) {
    os << ' ';
  }
}

template <
    int schedulerId,
    bool usingBatch,
    common::InputEncryption inputEncryption>
void AggMetrics<schedulerId, usingBatch, inputEncryption>::print(
    std::ostream& os,
    int32_t tabstop) const {
  printSpaces(os, tabstop);
  switch (getType()) {
    case AggMetricType::kDict: {
      os << "map{\n";
      tabstop += 2;
      for (const auto& [key, inner] : getAsDict()) {
        printSpaces(os, tabstop);
        os << key << ":\n";
        inner->print(os, tabstop);
      }
      tabstop -= 2;
      printSpaces(os, tabstop);
      os << "}\n";
      break;
    }
    case AggMetricType::kList: {
      os << "list[\n";
      tabstop += 2;
      for (const auto& inner : getAsList()) {
        inner->print(os, tabstop);
      }
      tabstop -= 2;
      printSpaces(os, tabstop);
      os << "]\n";
      break;
    }
    case AggMetricType::kValue: {
      os << "<" << getValue();
      if constexpr (inputEncryption == common::InputEncryption::Xor) {
        os << "> secretXor<" << schedulerId << "><";
      }
      os << ">\n";
      break;
    }
    default: {
      XLOG(ERR) << "metrics contains unsupported tag";
      throw common::exceptions::NotImplementedError(
          "This AggType does not exist");
      break;
    }
  }
}

template <
    int schedulerId,
    bool usingBatch,
    common::InputEncryption inputEncryption>
folly::dynamic AggMetrics<schedulerId, usingBatch, inputEncryption>::toDynamic()
    const {
  switch (getType()) {
    case AggMetricType::kDict: {
      folly::dynamic container = folly::dynamic::object();
      for (const auto& [key, value] : getAsDict()) {
        container.insert(key, value->toDynamic());
      }
      return container;
    }
    case AggMetricType::kList: {
      folly::dynamic container = folly::dynamic::array();
      std::transform(
          getAsList().begin(),
          getAsList().end(),
          std::back_inserter(container),
          [](auto m) { return m->toDynamic(); });
      return container;
    }
    case AggMetricType::kValue: {
      return getValue();
    }
    default:
      XLOG(ERR) << "Metric values should be maps, lists, or integers here";
      throw common::exceptions::NotImplementedError(
          "Metric values should be maps, lists, or integers here.");
  }
}

template <
    int schedulerId,
    bool usingBatch,
    common::InputEncryption inputEncryption>
folly::dynamic
AggMetrics<schedulerId, usingBatch, inputEncryption>::toRevealedDynamic(
    int party) const {
  if constexpr (inputEncryption == common::InputEncryption::Xor) {
    switch (getType()) {
      case AggMetricType::kDict: {
        folly::dynamic container = folly::dynamic::object();
        for (const auto& [key, value] : getAsDict()) {
          container.insert(key, value->toRevealedDynamic(party));
        }
        return container;
      }
      case AggMetricType::kList: {
        folly::dynamic container = folly::dynamic::array();
        std::transform(
            getAsList().begin(),
            getAsList().end(),
            std::back_inserter(container),
            [party](auto m) { return m->toRevealedDynamic(party); });
        return container;
      }
      case AggMetricType::kValue: {
        if constexpr (inputEncryption == common::InputEncryption::Xor) {
          if constexpr (usingBatch)
            return getSecValueXor().openToParty(party).getValue().at(0);
          else
            return getSecValueXor().openToParty(party).getValue();
        } else {
          return getValue();
        }
      }
      default:
        XLOG(ERR) << "Metric values should be maps, lists, or integers here";
        throw common::exceptions::NotImplementedError(
            "Metric values should be maps, lists, or integers here.");
    }
  } else {
    XLOG(ERR, "To reveal metrics it has to be encrypted as a Xor-SS");
    throw common::exceptions::InvalidAccessError(
        "To reveal metrics it has to be encrypted as a Xor-SS");
  }
}

} // namespace shard_combiner
