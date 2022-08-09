/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <unistd.h>
#include <type_traits>

#include <folly/Format.h>

#include <fbpcf/exception/exceptions.h>

#include "fbpcs/emp_games/common/Constants.h"
#include "fbpcs/emp_games/pcf2_shard_combiner/AggMetrics.h"
#include "fbpcs/emp_games/pcf2_shard_combiner/ShardValidator.h"
#include "fbpcs/emp_games/pcf2_shard_combiner/util/AggMetricsThresholdCheckers.h"

namespace shard_combiner {

/**
 * GroupedLiftMetrics holds LiftMetrics in the form:
 * {
 *   "metrics" : LiftMetrics{},
 *   "cohortMetrics" : [LiftMetrics{}, ...]
 *   "publisherBreakdowns" : [LiftMetrics{}, ...]
 * }
 * we break our operation into checking(checkLiftMetricsThreshold) and
 * masking(applyLiftMetricsThreshold) for each of the traversal.
 */
template <
    int schedulerId = 0,
    bool usingBatch = false,
    common::InputEncryption inputEncryption =
        common::InputEncryption::Plaintext>
ThresholdFn<schedulerId, usingBatch, inputEncryption> getGroupLiftChecker(
    int64_t threshold,
    int64_t sentinelVal) {
  auto myThresholdMetric =
      std::make_shared<AggMetrics<schedulerId, usingBatch, inputEncryption>>(
          AggMetricType::kValue);

  auto mySentinelMetric =
      std::make_shared<AggMetrics<schedulerId, usingBatch, inputEncryption>>(
          AggMetricType::kValue);

  myThresholdMetric->setValue(threshold);
  mySentinelMetric->setValue(sentinelVal);
  if constexpr (inputEncryption == common::InputEncryption::Xor) {
    myThresholdMetric->updateSecValueFromPublicInt();
    mySentinelMetric->updateSecValueFromPublicInt();
  } else if constexpr (inputEncryption == common::InputEncryption::Plaintext) {
    /* plaintext do nothing, setValue() should do. */
  }
  return [myThresholdMetric, mySentinelMetric](
             AggMetrics_sp<schedulerId, usingBatch, inputEncryption>
                 aggMetrics) {
    if (aggMetrics->getType() == AggMetricType::kDict) {
      auto metricsAggMetric = aggMetrics->getAtKey("metrics");
      // check and apply threshold for
      auto condition =
          checkLiftMetricsThreshold<schedulerId, usingBatch, inputEncryption>(
              metricsAggMetric, myThresholdMetric);
      applyLiftMetricsThreshold<schedulerId, usingBatch, inputEncryption>(
          metricsAggMetric, mySentinelMetric, condition);

      auto cohortMetrics = aggMetrics->getAtKey("cohortMetrics");
      for (const auto& cohortMetric : cohortMetrics->getAsList()) {
        auto cohortCondition =
            checkLiftMetricsThreshold<schedulerId, usingBatch, inputEncryption>(
                cohortMetric, myThresholdMetric);
        applyLiftMetricsThreshold<schedulerId, usingBatch, inputEncryption>(
            cohortMetric, mySentinelMetric, cohortCondition);
      }
      auto publisherBreakdownMetrics =
          aggMetrics->getAtKey("publisherBreakdowns");
      for (const auto& publisherBreakdownMetric :
           publisherBreakdownMetrics->getAsList()) {
        auto publisherBreakdownCondition =
            checkLiftMetricsThreshold<schedulerId, usingBatch, inputEncryption>(
                publisherBreakdownMetric, myThresholdMetric);
        applyLiftMetricsThreshold<schedulerId, usingBatch, inputEncryption>(
            publisherBreakdownMetric,
            mySentinelMetric,
            publisherBreakdownCondition);
      }
    }
  };
}

template <
    int schedulerId,
    bool usingBatch,
    common::InputEncryption inputEncryption>
void applyLiftMetricsThreshold(
    AggMetrics_sp<schedulerId, usingBatch, inputEncryption> aggMetrics,
    AggMetrics_sp<schedulerId, usingBatch, inputEncryption> sentinelMetric,
    const BitVariant<schedulerId, usingBatch>& condition) {
  if (aggMetrics->getType() == AggMetricType::kDict) {
    auto isNotIn = [](const std::string& is,
                      const std::vector<std::string>& in) -> bool {
      return std::find(in.begin(), in.end(), is) == in.end();
    };

    for (auto& [k, v] : aggMetrics->getAsDict()) {
      if (v->getType() == AggMetricType::kValue &&
          isNotIn(k, {"testPopulation", "controlPopulation"})) {
        v->mux(condition, sentinelMetric);
      }
    }
  }
}

template <
    int schedulerId,
    bool usingBatch,
    common::InputEncryption inputEncryption>
BitVariant<schedulerId, usingBatch> checkLiftMetricsThreshold(
    AggMetrics_sp<schedulerId, usingBatch, inputEncryption> aggMetrics,
    AggMetrics_sp<schedulerId, usingBatch, inputEncryption> thresholdMetric) {
  if (aggMetrics->getType() == AggMetricType::kDict) {
    auto testConverters = aggMetrics->getAtKey("testConverters");
    auto controlConverters = aggMetrics->getAtKey("controlConverters");

    auto result = AggMetrics<schedulerId, usingBatch, inputEncryption>::newLike(
        testConverters);
    result->updateSecValueFromRawInt();

    // check (controlConverters + testConverters) > threshold ?
    AggMetrics<schedulerId, usingBatch, inputEncryption>::accumulate(
        result, controlConverters);
    AggMetrics<schedulerId, usingBatch, inputEncryption>::accumulate(
        result, testConverters);

    return result->isGreaterOrEqual(*thresholdMetric);

  } else {
    std::string errStr = folly::sformat(
        "Type: {} not supported, has to be AggMetricsType::kDict.",
        static_cast<std::underlying_type<AggMetricType>::type>(
            aggMetrics->getType()));
    throw common::exceptions::InvalidAccessError(errStr);
  }
}

template <
    int schedulerId,
    bool usingBatch,
    common::InputEncryption inputEncryption>
std::function<void(AggMetrics_sp<schedulerId, usingBatch, inputEncryption>)>
checkThresholdAndUpdateMetric(
    ShardSchemaType shardSchemaType,
    int64_t threshold,
    int64_t sentinelVal = -1) {
  if (shardSchemaType == ShardSchemaType::kGroupedLiftMetrics) {
    return getGroupLiftChecker<schedulerId, usingBatch, inputEncryption>(
        threshold, sentinelVal);
  } else {
    return [threshold, sentinelVal](
               AggMetrics_sp<schedulerId, usingBatch, inputEncryption>) {
      // for any other type do nothing.
      XLOG(WARN) << "Threshold: " << threshold << " is unused";
    };
  }
}

} // namespace shard_combiner
