/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "fbpcs/emp_games/attribution/shard_aggregator/AggMetricsThresholdCheckers.h"

#include <fbpcf/common/FunctionalUtil.h>
#include <fbpcf/mpc/EmpGame.h>
#include "fbpcs/emp_games/attribution/Constants.h"
#include "fbpcs/emp_games/attribution/shard_aggregator/AggMetrics.h"

#include <stdexcept>

namespace {
using private_measurement::AggMetrics;

static constexpr int64_t kHiddenMetricConstant = -1;

// liftMetrics should store a MetricsMap containing all the lift metrics
void hideIfConditionFails(
    std::shared_ptr<AggMetrics> value,
    emp::Bit& condition,
    emp::Integer& hiddenMetric) {
  if (value->getTag() == private_measurement::AggMetricsTag::EmpInteger) {
    value->setEmpIntValue(
        emp::If(condition, value->getEmpIntValue(), hiddenMetric));
  } else if (value->getTag() == private_measurement::AggMetricsTag::List) {
    for (auto& innerValue : value->getAsList()) {
      hideIfConditionFails(innerValue, condition, hiddenMetric);
    }
  } else if (value->getTag() == private_measurement::AggMetricsTag::Map) {
    for (auto& [_, innerValue] : value->getAsMap()) {
      hideIfConditionFails(innerValue, condition, hiddenMetric);
    }
  } else {
    throw std::invalid_argument{"Unexpected AggmetricsTag::Integer"};
  }
}

void applyLiftThresholdCondition(
    std::shared_ptr<AggMetrics> liftMetrics,
    emp::Integer kAnonymityLevel,
    emp::Integer hiddenMetric) {
  auto condition = liftMetrics->getAtKey("testConverters")->getEmpIntValue() +
          liftMetrics->getAtKey("controlConverters")->getEmpIntValue() >=
      kAnonymityLevel;
  for (const auto& [key, value] : liftMetrics->getAsMap()) {
    if (key == "controlPopulation" || key == "testPopulation") {
      // These two values are always revealed
      continue;
    } else {
      // Call the hide function which will recursively hide metrics if the
      // condition above failed the anonymity check
      hideIfConditionFails(value, condition, hiddenMetric);
    }
  }
}

void findLiftThresholdConditionValidNodes(
    std::shared_ptr<AggMetrics> metrics,
    emp::Integer kAnonymityLevel,
    emp::Integer hiddenMetric) {
  if (metrics->getTag() == private_measurement::AggMetricsTag::List) {
    for (auto& innerValue : metrics->getAsList()) {
      findLiftThresholdConditionValidNodes(
          innerValue, kAnonymityLevel, hiddenMetric);
    }
  } else if (metrics->getTag() == private_measurement::AggMetricsTag::Map) {
    auto& innerMap = metrics->getAsMap();
    if (innerMap.find("testConverters") != innerMap.end() &&
        innerMap.find("controlConverters") != innerMap.end()) {
      // We found a valid inner node, call the apply function
      applyLiftThresholdCondition(metrics, kAnonymityLevel, hiddenMetric);
    } else {
      // Otherwise, we need to keep iterating inside to see if there might be
      // a valid node deeper within the structure
      for (auto& [_, innerValue] : innerMap) {
        findLiftThresholdConditionValidNodes(
            innerValue, kAnonymityLevel, hiddenMetric);
      }
    }
  }
}
} // namespace

namespace measurement::private_attribution {
std::function<void(std::shared_ptr<AggMetrics>)> constructLiftThresholdChecker(
    int64_t threshold) {
  return [threshold](std::shared_ptr<AggMetrics> metrics) {
    const emp::Integer hiddenMetric{
        INT_SIZE, kHiddenMetricConstant, emp::PUBLIC};
    const emp::Integer kAnonymityLevel{
        private_measurement::INT_SIZE, threshold, emp::PUBLIC};

    for (auto& [key, value] : metrics->getAsMap()) {
      findLiftThresholdConditionValidNodes(
          value, kAnonymityLevel, hiddenMetric);
    }
  };
}

std::function<void(std::shared_ptr<AggMetrics>)>
constructAdObjectFormatThresholdChecker(int64_t threshold) {
  return [threshold](std::shared_ptr<AggMetrics> metrics) {
    const emp::Integer hiddenMetric{
        INT_SIZE, kHiddenMetricConstant, emp::PUBLIC};
    const emp::Integer kAnonymityLevel{
        private_measurement::INT_SIZE, threshold, emp::PUBLIC};

    for (const auto& [rule, metricsMap] : metrics->getAsMap()) {
      for (const auto& [aggregationName, aggregationData] :
           metricsMap->getAsMap()) {
        for (const auto& [id, metrics] : aggregationData->getAsMap()) {
          auto condition =
              metrics->getAtKey("convs")->getEmpIntValue() >= kAnonymityLevel;
          metrics->getAtKey("sales")->setEmpIntValue(emp::If(
              condition,
              metrics->getAtKey("sales")->getEmpIntValue(),
              hiddenMetric));
          metrics->getAtKey("convs")->setEmpIntValue(emp::If(
              condition,
              metrics->getAtKey("convs")->getEmpIntValue(),
              hiddenMetric));
        }
      }
    }
  };
}
} // namespace measurement::private_attribution
