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

namespace {
using private_measurement::AggMetrics;

static constexpr int64_t kHiddenMetricConstant = -1;

// liftMetrics should store a MetricsMap containing all the lift metrics
void applyLiftThresholdCondition(
    std::shared_ptr<AggMetrics> liftMetrics,
    emp::Integer kAnonymityLevel,
    emp::Integer hiddenMetric) {
  auto condition = liftMetrics->getAtKey("testConverters")->getEmpIntValue() +
          liftMetrics->getAtKey("controlConverters")->getEmpIntValue() >=
      kAnonymityLevel;
  for (const auto& [key, value] : liftMetrics->getAsMap()) {
    // TODO: Use std::string operator== instead of compare
    if (!key.compare("controlPopulation") || !key.compare("testPopulation")) {
      // These two values are always revealed
      continue;
    }
    else if (key == "controlConvHistogram" || key == "testConvHistogram") {
      auto& innerList = value->getAsList();
      for (auto& innerValue : innerList) {
        innerValue->setEmpIntValue(
          emp::If(condition, innerValue->getEmpIntValue(), hiddenMetric));
      }
    } else {
      value->setEmpIntValue(
          emp::If(condition, value->getEmpIntValue(), hiddenMetric));
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

    // apply to metrics
    applyLiftThresholdCondition(
        metrics->getAtKey("metrics"), kAnonymityLevel, hiddenMetric);

    // apply to cohort metrics
    for (const auto& cohort : metrics->getAtKey("cohortMetrics")->getAsList()) {
      applyLiftThresholdCondition(cohort, kAnonymityLevel, hiddenMetric);
    }

    // apply to publisher breakdowns
    for (const auto& breakdown : metrics->getAtKey("publisherBreakdowns")->getAsList()) {
      applyLiftThresholdCondition(breakdown, kAnonymityLevel, hiddenMetric);
    }
  };
}

std::function<void(std::shared_ptr<AggMetrics>)> constructAdObjectFormatThresholdChecker(
    int64_t threshold) {
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
