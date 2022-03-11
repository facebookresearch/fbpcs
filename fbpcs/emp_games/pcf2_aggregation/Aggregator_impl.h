/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <algorithm>
#include <cmath>
#include <iterator>
#include <memory>
#include <string>
#include <utility>
#include "folly/dynamic.h"

#include "fbpcs/emp_games/common/Constants.h"
#include "fbpcs/emp_games/pcf2_aggregation/Constants.h"

namespace pcf2_aggregation {

namespace {

template <int schedulerId>
struct MeasurementAggregation {
  // ad_id => metrics
  std::unordered_map<int64_t, ConvMetrics> metrics;

  // struct to store the touchpoint-conversion pairs.
  struct PrivateMeasurementAggregationResult {
    SecBit<schedulerId> hasAttributedTouchpoint;
    PrivateMeasurementConversionMetadata<schedulerId>
        measurementConversionMetadata;
    PrivateMeasurementTouchpointMetadata<schedulerId>
        measurementTouchpointMetadata;
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

template <int schedulerId>
class MeasurementAggregator : public Aggregator<schedulerId> {
 public:
  explicit MeasurementAggregator(
      const std::vector<uint64_t>& validOriginalAdIds,
      const common::Visibility& outputVisibility,
      const int myRole,
      const int concurrency,
      std::unique_ptr<fbpcf::mpc_std_lib::oram::IWriteOnlyOramFactory<
          fbpcf::mpc_std_lib::util::AggregationValue>> writeOnlyOramFactory)
      : Aggregator<schedulerId>{outputVisibility} {
    _validOriginalAdIds = validOriginalAdIds;
    size_t oramSize = _validOriginalAdIds.size() + 1;
    // Note that oramSize must be nonzero because
    // we will be taking its logarithm.
    CHECK_GT(oramSize, 0) << "ORAM size must be greater than zero.";
    _oramWidth =
        std::ceil(std::log2(oramSize)); // number of bits used to store the adId
    _writeOnlyOram = writeOnlyOramFactory->create(oramSize);
    _oramMaxBatchSize =
        writeOnlyOramFactory->getMaxBatchSize(oramSize, concurrency);
    XLOGF(INFO, "ORAM maxBatchSize = {}", _oramMaxBatchSize);
  }

  virtual void aggregateAttributions(
      const PrivateAggregation<schedulerId>& privateAggregation) override {
    XLOG(INFO, "Computing measurement aggregation based on attributions...");
    const auto& privateTpmArrays = privateAggregation.privateTpm;
    const auto& privateCvmArrays = privateAggregation.privateCvm;
    const auto& privateAttributionArrays =
        privateAggregation.attributionResults;
    XLOGF(
        DBG,
        "For measurement aggregator, size of attribution: {}, tp metadata: {}, conv metadata: {}",
        privateAttributionArrays.size(),
        privateTpmArrays.size(),
        privateCvmArrays.size());

    CHECK_EQ(privateAttributionArrays.size(), privateTpmArrays.size())
        << "Size of attribution results and touchpoint metadata should be equal.";
    CHECK_EQ(privateCvmArrays.size(), privateTpmArrays.size())
        << "Size of conversion metadata and touchpoint metadata should be equal.";

    std::vector<std::vector<typename MeasurementAggregation<
        schedulerId>::PrivateMeasurementAggregationResult>>
        touchpointConversionResults;
    for (size_t i = 0; i < privateCvmArrays.size(); ++i) {
      // Retrieve the touchpoint-conversion metadata pairs based on
      // attribution results.
      auto touchpointConversionResultsPerId =
          retrieveTouchpointForConversionPerID(
              privateTpmArrays.at(i),
              privateCvmArrays.at(i),
              privateAttributionArrays.at(i));
      touchpointConversionResults.push_back(touchpointConversionResultsPerId);
    }

    XLOG(INFO, "Retrieved touchpoint-conversion metadata");

    // Use ORAM for aggregation
    aggregateUsingOram(touchpointConversionResults);
  }

  const std::vector<typename MeasurementAggregation<
      schedulerId>::PrivateMeasurementAggregationResult>
  retrieveTouchpointForConversionPerID(
      const std::vector<PrivateMeasurementTouchpointMetadata<schedulerId>>&
          privateTpmArray,
      const std::vector<PrivateMeasurementConversionMetadata<schedulerId>>&
          privateCvmArray,
      const std::vector<PrivateAttributionResult<schedulerId>>&
          attributionResults) {
    std::vector<typename MeasurementAggregation<
        schedulerId>::PrivateMeasurementAggregationResult>
        aggregationResults;
    int numOfResults = privateTpmArray.size() - 1;
    int atIndex = attributionResults.size() - 1;

    for (auto convIndex = numOfResults; convIndex >= 0; convIndex--) {
      SecBit<schedulerId> hasAttributedTouchpoint(false, common::PUBLISHER);
      uint8_t defaultAdId = 0;
      SecAdId<schedulerId> attributedAdId(defaultAdId, common::PUBLISHER);

      for (auto tpIndex = numOfResults; tpIndex >= 0; tpIndex--) {
        auto isAttributed = !hasAttributedTouchpoint &
            attributionResults.at(atIndex).isAttributed;

        hasAttributedTouchpoint = hasAttributedTouchpoint || isAttributed;

        attributedAdId =
            attributedAdId.mux(isAttributed, privateTpmArray.at(tpIndex).adId);

        atIndex--;
      }

      typename MeasurementAggregation<
          schedulerId>::PrivateMeasurementAggregationResult aggregationResult{
          /* hasAttributedTouchpoint */ hasAttributedTouchpoint,
          /* conv */ privateCvmArray.at(convIndex),
          /* tp */
          PrivateMeasurementTouchpointMetadata<schedulerId>{attributedAdId}};

      aggregationResults.push_back(aggregationResult);
    }
    return aggregationResults;
  }

  void aggregateUsingOram(
      const std::vector<std::vector<typename MeasurementAggregation<
          schedulerId>::PrivateMeasurementAggregationResult>>&
          touchpointConversionResults) {
    size_t startIndex = 0;
    while (startIndex < touchpointConversionResults.size()) {
      size_t endIndex = std::min(
          startIndex + _oramMaxBatchSize, touchpointConversionResults.size());
      XLOGF(
          INFO,
          "ORAM batch startIndex = {}, endIndex = {}",
          startIndex,
          endIndex);
      auto oramInput =
          generateOramInput(touchpointConversionResults, startIndex, endIndex);
      _writeOnlyOram->obliviousAddBatch(oramInput.first, oramInput.second);
      startIndex = endIndex;
    }
  }

  /**
   * Generate input to ORAM from touchpointConversionResults, between startIndex
   * and endIndex.
   **/
  const std::
      pair<std::vector<std::vector<bool>>, std::vector<std::vector<bool>>>
      generateOramInput(
          const std::vector<std::vector<typename MeasurementAggregation<
              schedulerId>::PrivateMeasurementAggregationResult>>&
              touchpointConversionResults,
          const size_t startIndex,
          const size_t endIndex) {
    std::vector<std::vector<bool>> indexShares(_oramWidth, std::vector<bool>{});
    std::vector<std::vector<bool>> valueShares(
        salesValueWidth + convValueWidth, std::vector<bool>{});

    CHECK_LT(startIndex, touchpointConversionResults.size())
        << "ORAM startIndex must be less than size of array";
    CHECK_LE(endIndex, touchpointConversionResults.size())
        << "ORAM endIndex must be at most size of array";
    for (size_t i = startIndex; i < endIndex; ++i) {
      for (auto& touchpointConversionResult :
           touchpointConversionResults.at(i)) {
        const auto& touchpoint =
            touchpointConversionResult.measurementTouchpointMetadata;
        const auto& conversion =
            touchpointConversionResult.measurementConversionMetadata;
        // Retrieve adId shares
        auto indexShare = touchpoint.adId.extractIntShare().getBooleanShares();
        for (size_t i = 0; i < _oramWidth; ++i) {
          indexShares.at(i).push_back(indexShare.at(i));
        }
        // Retrieve conversion value share if attributed, or zero if not
        // attributed
        const PubSalesValue<schedulerId> one(uint32_t(1));
        const PubConvValue<schedulerId> zero(uint32_t(0));
        auto salesValue =
            zero.mux(touchpointConversionResult.hasAttributedTouchpoint, one);
        auto convValue = zero.mux(
            touchpointConversionResult.hasAttributedTouchpoint,
            conversion.convValue);
        auto salesValueShare = salesValue.extractIntShare().getBooleanShares();
        auto convValueShare = convValue.extractIntShare().getBooleanShares();
        for (size_t i = 0; i < salesValueWidth; ++i) {
          valueShares.at(i).push_back(salesValueShare.at(i));
        }
        for (size_t j = 0; j < convValueWidth; j++) {
          valueShares.at(j + salesValueWidth).push_back(convValueShare.at(j));
        }
      }
    }
    return std::make_pair(std::move(indexShares), std::move(valueShares));
  }

  virtual AggregationOutput reveal() const override {
    MeasurementAggregation<schedulerId> out;
    for (size_t i = 1; i < _validOriginalAdIds.size() + 1; ++i) {
      const auto rAdId = _validOriginalAdIds.at(i - 1);
      XLOGF(DBG, "Revealing measurement metrics for adId={}", rAdId);
      fbpcf::mpc_std_lib::util::AggregationValue aggregationValue;
      if (Aggregator<schedulerId>::outputVisibility_ ==
          common::Visibility::Publisher) {
        aggregationValue = _writeOnlyOram->publicRead(
            i,
            fbpcf::mpc_std_lib::oram::IWriteOnlyOram<
                fbpcf::mpc_std_lib::util::AggregationValue>::Alice);
        out.metrics[rAdId] = ConvMetrics{
            aggregationValue.conversionCount, aggregationValue.conversionValue};
      } else {
        auto additiveAggregationValue = _writeOnlyOram->secretRead(i);

        // Convert additive shares to secret shares by inputting them into MPC
        // and adding them, then extracting the secret shares.
        auto publisherConvs = SecConvValue<schedulerId>(
            additiveAggregationValue.conversionCount, common::PUBLISHER);
        auto partnerConvs = SecConvValue<schedulerId>(
            additiveAggregationValue.conversionCount, common::PARTNER);
        auto convs = publisherConvs + partnerConvs;
        auto extractedConvs = convs.extractIntShare().getValue();

        auto publisherSales = SecSalesValue<schedulerId>(
            additiveAggregationValue.conversionValue, common::PUBLISHER);
        auto partnerSales = SecSalesValue<schedulerId>(
            additiveAggregationValue.conversionValue, common::PARTNER);
        auto sales = publisherSales + partnerSales;
        auto extractedSales = sales.extractIntShare().getValue();

        out.metrics[rAdId] = ConvMetrics{
            static_cast<uint32_t>(extractedConvs),
            static_cast<uint32_t>(extractedSales)};
      }
    }
    return out.toDynamic();
  }

 private:
  std::vector<uint64_t> _validOriginalAdIds;
  std::unique_ptr<fbpcf::mpc_std_lib::oram::IWriteOnlyOram<
      fbpcf::mpc_std_lib::util::AggregationValue>>
      _writeOnlyOram;
  uint32_t _oramMaxBatchSize;
  uint8_t _oramWidth;
};
} // namespace

template <int schedulerId>
static const std::vector<AggregationFormat<schedulerId>>
    SUPPORTED_AGGREGATION_FORMATS{AggregationFormat<schedulerId>{
        /* id */ 1,
        /* name */ common::MEASUREMENT,
        /* newAggregator */
        [](AggregationContext ctx,
           common::Visibility outputVisibility,
           int myRole,
           int concurrency,
           std::unique_ptr<fbpcf::mpc_std_lib::oram::IWriteOnlyOramFactory<
               fbpcf::mpc_std_lib::util::AggregationValue>>
               writeOnlyOramFactory)
            -> std::unique_ptr<Aggregator<schedulerId>> {
          return std::make_unique<MeasurementAggregator<schedulerId>>(
              ctx.validOriginalAdIds,
              outputVisibility,
              myRole,
              concurrency,
              std::move(writeOnlyOramFactory));
        }}};

template <int schedulerId>
const AggregationFormat<schedulerId>
AggregationFormat<schedulerId>::fromNameOrThrow(const std::string& name) {
  for (auto rule : SUPPORTED_AGGREGATION_FORMATS<schedulerId>) {
    if (rule.name == name) {
      return rule;
    }
  }

  throw std::runtime_error("Unknown aggregation format name: " + name);
}

template <int schedulerId>
const AggregationFormat<schedulerId>
AggregationFormat<schedulerId>::fromIdOrThrow(int64_t id) {
  for (auto rule : SUPPORTED_AGGREGATION_FORMATS<schedulerId>) {
    if (rule.id == id) {
      return rule;
    }
  }

  throw std::runtime_error(fmt::format("Unknown aggregation id: {}", id));
}

} // namespace pcf2_aggregation
