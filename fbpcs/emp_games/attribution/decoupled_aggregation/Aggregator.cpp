/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <algorithm>
#include <iterator>
#include <memory>
#include <string>
#include <utility>
#include "folly/dynamic.h"

#include "fbpcs/emp_games/common/EmpOperationUtil.h"
#include "fbpcs/emp_games/common/PrivateData.h"
#include "fbpcs/emp_games/common/SecretSharing.h"

#include "fbpcs/emp_games/attribution/decoupled_aggregation/Aggregator.h"
#include "fbpcs/emp_games/attribution/decoupled_aggregation/Constants.h"

namespace aggregation::private_aggregation {

using PrivateConvMap = std::vector<std::pair<emp::Integer, PrivateConvMetrics>>;

namespace {

struct MeasurementAggregation {
  // ad_id => metrics
  std::unordered_map<int64_t, ConvMetrics> metrics;

  // struct to store the touchpoint-conversion pairs.
  struct PrivateMeasurementAggregationResult {
    emp::Bit hasAttributedTouchpoint;
    PrivateMeasurementConversionMetadata measurementConversionMetadata;
    PrivateMeasurementTouchpointMetadata measurementTouchpointMetadata;
  };

  folly::dynamic toDynamic() const {
    folly::dynamic res = folly::dynamic::object();

    for (const auto& [k, v] : metrics) {
      auto key = std::to_string(k);
      auto val = v.toDynamic();
      res.insert(key, val);
    }

    return res;
  }
};

class MeasurementAggregator : public Aggregator {
 public:
  explicit MeasurementAggregator(
      const std::vector<int64_t>& validAdIds,
      const fbpcf::Visibility& outputVisibility)
      : Aggregator{outputVisibility} {
    for (auto adId : validAdIds) {
      _adIdToMetrics.push_back(std::make_pair(
          emp::Integer{INT_SIZE, adId, emp::PUBLIC}, PrivateConvMetrics{}));
    }
  }

  virtual void aggregateAttributions(
      const PrivateAggregation& privateAggregation) override {
    XLOG(INFO, "Computing measurement aggregation based on attributions...");
    const auto& privateTpmArrays = privateAggregation.privateTpm;
    const auto& privateCvmArrays = privateAggregation.privateCvm;
    const auto& privateTpAttributionArrays =
        privateAggregation.tpAttributionResults;
    const auto& privateCvmAttributionsArrays =
        privateAggregation.convAttributionResults;
    XLOGF(
        DBG,
        "For measurement aggregator, size of tpAttribution: {}, conversion attribution: {}, tp metadata: {}, conv metadata: {}",
        privateTpAttributionArrays.size(),
        privateCvmAttributionsArrays.size(),
        privateTpmArrays.size(),
        privateCvmArrays.size());

    CHECK_EQ(privateTpAttributionArrays.size(), privateTpmArrays.size())
        << "Size of touchpoint attribution results and touchpoint metadata should be equal.";
    CHECK_EQ(privateCvmAttributionsArrays.size(), privateTpmArrays.size())
        << "Size of conversion attribution results and touchpoint metadata should be equal.";
    CHECK_EQ(privateCvmArrays.size(), privateTpmArrays.size())
        << "Size of conversion metadata and touchpoint metadata should be equal.";

    std::vector<std::vector<
        MeasurementAggregation::PrivateMeasurementAggregationResult>>
        touchpointConversionResults;
    for (std::size_t i = 0; i < privateCvmArrays.size(); i++) {
      // Retrieve the touchpoint-conversion metadata pairs based on attribution
      // results. One assumption here is that one conversion will only be
      // attributed to one touchpoint.
      auto touchpointConversionResultsPerId =
          retrieveTouchpointForConversionPerID(
              privateTpmArrays.at(i),
              privateCvmArrays.at(i),
              privateTpAttributionArrays.at(i),
              privateCvmAttributionsArrays.at(i));
      touchpointConversionResults.push_back(touchpointConversionResultsPerId);
    }

    for (auto& touchpointConversionResultsPerId : touchpointConversionResults) {
      for (auto& touchpointConversionResult :
           touchpointConversionResultsPerId) {
        const auto& touchpoint =
            touchpointConversionResult.measurementTouchpointMetadata;
        const auto& conversion =
            touchpointConversionResult.measurementConversionMetadata;

        for (auto& [adId, metrics] : _adIdToMetrics) {
          const emp::Integer zero{INT_SIZE_32, 0, emp::PUBLIC};
          const emp::Integer one{INT_SIZE_32, 1, emp::PUBLIC};

          const auto adIdMatches =
              touchpointConversionResult.hasAttributedTouchpoint &
              adId.equal(touchpoint.adId);

          // emp::If(condition, true_case, false_case)
          const auto convsDelta = emp::If(adIdMatches, one, zero);
          const auto salesDelta =
              emp::If(adIdMatches, conversion.conv_value, zero);

          metrics.convs = metrics.convs + convsDelta;
          metrics.sales = metrics.sales + salesDelta;
        }
      }
    }
  }

  const std::vector<MeasurementAggregation::PrivateMeasurementAggregationResult>
  retrieveTouchpointForConversionPerID(
      const std::vector<PrivateMeasurementTouchpointMetadata>& tpmArray,
      const std::vector<PrivateMeasurementConversionMetadata>& cvmArray,
      const std::vector<PrivateAttributionResult>& tpmAttributionResults,
      const std::vector<PrivateAttributionResult>& cvmAttributionResults) {
    std::vector<MeasurementAggregation::PrivateMeasurementAggregationResult>
        aggregationResults;
    int numOfResults = tpmArray.size() - 1;
    int atIndex = tpmAttributionResults.size() - 1;
    for (auto convIndex = numOfResults; convIndex >= 0; convIndex--) {
      // Start with an unattributed attribution
      auto hasAttributedTouchpoint = emp::Bit{false};
      MeasurementAggregation::PrivateMeasurementAggregationResult
          aggregationResult{
              /* hasAttributedTouchpoint */ emp::Bit{false},
              /* conv */ cvmArray.at(convIndex),
              /* tp */ PrivateMeasurementTouchpointMetadata{}};

      for (auto tpIndex = numOfResults; tpIndex >= 0; tpIndex--) {
        auto isAttributed = !aggregationResult.hasAttributedTouchpoint &
            (cvmAttributionResults.at(atIndex).isAttributed ^
             tpmAttributionResults.at(atIndex).isAttributed);

        aggregationResult =
            MeasurementAggregation::PrivateMeasurementAggregationResult{
                /* hasAttributedTouchpoint */ aggregationResult
                        .hasAttributedTouchpoint |
                    isAttributed,
                /* conv */ cvmArray.at(convIndex),
                /* tp */
                aggregationResult.measurementTouchpointMetadata.select(
                    isAttributed, tpmArray.at(tpIndex))};
        atIndex--;
      }

      aggregationResults.push_back(aggregationResult);
    }
    return aggregationResults;
  }

  virtual AggregationOutput reveal() const override {
    MeasurementAggregation out;
    for (auto& [adId, metrics] : _adIdToMetrics) {
      const auto rAdId = adId.reveal<int64_t>();
      XLOGF(DBG, "Revealing measurement metrics for adId={}", rAdId);
      const auto rMetrics = metrics.reveal(outputVisibility_);
      out.metrics[rAdId] = rMetrics;
    }

    return out.toDynamic();
  }

 private:
  PrivateConvMap _adIdToMetrics;
};
} // namespace

static const std::array SUPPORTED_AGGREGATION_FORMATS{AggregationFormat{
    /* id */ 1,
    /* name */ "measurement",
    /* newAggregator */
    [](AggregationContext ctx,
       fbpcf::Visibility outputVisibility) -> std::unique_ptr<Aggregator> {
      return std::make_unique<MeasurementAggregator>(
          ctx.validAdIds, outputVisibility);
    }}};

AggregationFormat getAggregationFormatFromNameOrThrow(const std::string& name) {
  for (auto rule : SUPPORTED_AGGREGATION_FORMATS) {
    if (rule.name == name) {
      return rule;
    }
  }

  throw std::runtime_error("Unknown aggregation rule name: " + name);
}

AggregationFormat getAggregationFormatFromIdOrThrow(int64_t id) {
  for (auto rule : SUPPORTED_AGGREGATION_FORMATS) {
    if (rule.id == id) {
      return rule;
    }
  }

  throw std::runtime_error(fmt::format("Unknown aggregation id: {}", id));
}

} // namespace aggregation::private_aggregation
