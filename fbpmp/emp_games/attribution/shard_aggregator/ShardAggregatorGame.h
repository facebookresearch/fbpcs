/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <algorithm>
#include <cstdlib>
#include <memory>
#include <vector>

#include <folly/logging/xlog.h>

#include <fbpcf/common/VectorUtil.h>
#include <fbpcf/mpc/EmpGame.h>
#include <fbpcf/mpc/EmpVector.h>
#include "fbpmp/emp_games/attribution/Aggregator.h"
#include "fbpmp/emp_games/attribution/AttributionMetrics.h"
#include "fbpmp/emp_games/attribution/shard_aggregator/AggMetrics.h"

namespace measurement::private_attribution {
template <class IOChannel>
class ShardAggregatorGame
    : public fbpcf::EmpGame<
          IOChannel,
          std::vector<std::shared_ptr<private_measurement::AggMetrics>>,
          std::shared_ptr<private_measurement::AggMetrics>> {
 public:
  fbpcf::Party party_;

  ShardAggregatorGame(
      std::unique_ptr<IOChannel> ioChannel,
      fbpcf::Party party,
      std::optional<
          std::function<void(std::shared_ptr<private_measurement::AggMetrics>)>>
          thresholdChecker = std::nullopt,
      fbpcf::Visibility visibility = fbpcf::Visibility::Public)
      : fbpcf::EmpGame<
            IOChannel,
            std::vector<std::shared_ptr<private_measurement::AggMetrics>>,
            std::shared_ptr<private_measurement::AggMetrics>>(
            std::move(ioChannel),
            party),
        party_{party},
        visibility_{visibility},
        thresholdChecker_{thresholdChecker.value_or(
            [](std::shared_ptr<private_measurement::AggMetrics>
                   metrics /* unused */) {})} {}

  static constexpr int64_t kHiddenMetricConstant = -1;
  static constexpr int64_t kAnonymityThreshold = 100;

  std::shared_ptr<private_measurement::AggMetrics> play(
      const vector<std::shared_ptr<private_measurement::AggMetrics>>& inputData)
      override {
    vector<std::shared_ptr<private_measurement::AggMetrics>>
        reconstructedMetrics;

    // apply reconstruct function
    for (const auto& metrics : inputData) {
      reconstructedMetrics.push_back(applyReconstruct(metrics));
    }

    // aggregate everything
    auto result = applyAggregate(reconstructedMetrics);

    thresholdChecker_(result);
    return result;
  }

  std::shared_ptr<private_measurement::AggMetrics> applyReconstruct(
      const std::shared_ptr<private_measurement::AggMetrics>& metrics) const {
    if (metrics->getTag() == private_measurement::AggMetricsTag::Map) {
      // map stores keys in sorted order, so parties will access keys in the
      // same order
      auto reconstructedMetrics =
          std::make_shared<private_measurement::AggMetrics>(
              private_measurement::AggMetricsTag::Map);
      for (const auto& [key, value] : metrics->getAsMap()) {
        reconstructedMetrics->emplace(key, applyReconstruct(value));
      }
      return reconstructedMetrics;

    } else if (metrics->getTag() == private_measurement::AggMetricsTag::List) {
      auto reconstructedMetrics =
          std::make_shared<private_measurement::AggMetrics>(
              private_measurement::AggMetricsTag::List);
      for (const auto& m : metrics->getAsList()) {
        reconstructedMetrics->pushBack(applyReconstruct(m));
      }
      return reconstructedMetrics;

    } else if (
        metrics->getTag() == private_measurement::AggMetricsTag::Integer) {
      // XOR the integers
      auto alice = emp::Integer{INT_SIZE, metrics->getIntValue(), emp::ALICE};
      auto bob = emp::Integer{INT_SIZE, metrics->getIntValue(), emp::BOB};
      return std::make_shared<private_measurement::AggMetrics>(
          private_measurement::AggMetrics{alice ^ bob});

    } else {
      XLOG(FATAL)
          << "AggMetrics should only store a map, list, or int at this point";
    }
  }

  // uses the first metrics object as the accumulator
  std::shared_ptr<private_measurement::AggMetrics> applyAggregate(
      std::vector<std::shared_ptr<private_measurement::AggMetrics>>
          metricsVector) const {
    if (metricsVector.size() == 0) {
      return std::make_shared<private_measurement::AggMetrics>(
          private_measurement::AggMetrics{
              private_measurement::AggMetricsTag::Map});
    }
    auto accumulator =
        private_measurement::AggMetrics::copy(metricsVector.at(0));
    for (std::size_t i = 1; i < metricsVector.size(); ++i) {
      accumulator->mergeWithViaAddition(metricsVector.at(i));
    }
    return accumulator;
  }

 private:
  fbpcf::Visibility visibility_;
  std::function<void(std::shared_ptr<private_measurement::AggMetrics>)>
      thresholdChecker_;
};
} // namespace measurement::private_attribution
