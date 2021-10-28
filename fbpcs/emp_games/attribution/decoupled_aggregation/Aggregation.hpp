/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <functional>
#include <sstream>
#include <string>
#include <unordered_set>
#include <vector>

#include <fbpcf/mpc/EmpGame.h>
#include "folly/json.h"
#include "folly/logging/xlog.h"

#include "fbpcs/emp_games/common/EmpOperationUtil.h"
#include "fbpcs/emp_games/common/PrivateData.h"
#include "fbpcs/emp_games/common/SecretSharing.h"

#include "fbpcs/emp_games/attribution/decoupled_aggregation/AggregationMetrics.h"
#include "fbpcs/emp_games/attribution/decoupled_aggregation/AggregationOptions.h"
#include "fbpcs/emp_games/attribution/decoupled_aggregation/Aggregator.h"
#include "fbpcs/emp_games/attribution/decoupled_aggregation/AttributionResult.h"
#include "fbpcs/emp_games/attribution/decoupled_aggregation/Constants.h"
#include "fbpcs/emp_games/attribution/decoupled_aggregation/ConversionMetadata.h"
#include "fbpcs/emp_games/attribution/decoupled_aggregation/TouchPointMetadata.h"

namespace aggregation::private_aggregation {

const AttributionResult ATTRIBUTION_RESULTS_PADDING_VALUE{
    /* is_attributed */ false};

const MeasurementTouchpointMedata MEASUREMENT_TOUCHPOINT_PADDING_VALUE{
    /* ad_id */ -1};

const MeasurementConversionMetadata MEASUREMENT_CONVERSION_PADDING_VALUE{
    /* conv_value */ -1};

// privately sharing attribution results from publisher side.
template <int MY_ROLE>
const auto privatelyShareAttributionResultsTouchpoints = std::bind(
    private_measurement::secret_sharing::privatelyShareArraysFromAlice<
        MY_ROLE,
        AttributionResult,
        PrivateAttributionResult>,
    std::placeholders::_1,
    std::placeholders::_2,
    FLAGS_max_num_touchpoints* FLAGS_max_num_conversions,
    ATTRIBUTION_RESULTS_PADDING_VALUE);

// privately sharing attribution results from partner side.
template <int MY_ROLE>
const auto privatelyShareAttributionResultsConversions = std::bind(
    private_measurement::secret_sharing::privatelyShareArraysFromBob<
        MY_ROLE,
        AttributionResult,
        PrivateAttributionResult>,
    std::placeholders::_1,
    std::placeholders::_2,
    FLAGS_max_num_touchpoints* FLAGS_max_num_conversions,
    ATTRIBUTION_RESULTS_PADDING_VALUE);

// sharing touchpoint metadata - input/output values passed based on aggregation
// format.
template <int MY_ROLE, typename T, typename O>
const auto privatelyShareTouchpoints = std::bind(
    private_measurement::secret_sharing::
        privatelyShareArraysFromAlice<MY_ROLE, T, O>,
    std::placeholders::_1,
    std::placeholders::_2,
    FLAGS_max_num_touchpoints,
    MEASUREMENT_TOUCHPOINT_PADDING_VALUE);

// sharing conversion metadata - input/output values passed based on aggregation
// format.
template <int MY_ROLE, typename T, typename O>
const auto privatelyShareConversions = std::bind(
    private_measurement::secret_sharing::
        privatelyShareArraysFromBob<MY_ROLE, T, O>,
    std::placeholders::_1,
    std::placeholders::_2,
    FLAGS_max_num_conversions,
    MEASUREMENT_CONVERSION_PADDING_VALUE);

// get touchpoint metadata for ad_object aggregator format.
inline std::vector<std::vector<MeasurementTouchpointMedata>>
populateMeasurementTouchpointMetadata(
    const std::vector<std::vector<TouchpointMetadata>>&
        touchpointMetadataArrays) {
  std::vector<std::vector<MeasurementTouchpointMedata>> measurementTpmArrays;
  for (const std::vector<TouchpointMetadata>& touchpointMetadataArray :
       touchpointMetadataArrays) {
    std::vector<MeasurementTouchpointMedata> measurementTpmArray;
    for (const TouchpointMetadata& touchpointMetadata :
         touchpointMetadataArray) {
      measurementTpmArray.push_back(
          MeasurementTouchpointMedata{touchpointMetadata.adId});
    }
    measurementTpmArrays.push_back(measurementTpmArray);
  }
  return measurementTpmArrays;
}

// get conversion metadata for ad_object aggregator format.
inline std::vector<std::vector<MeasurementConversionMetadata>>
populateMeasurementConversionMetadata(
    const std::vector<std::vector<ConversionMetadata>>&
        conversionMetadataArrays) {
  std::vector<std::vector<MeasurementConversionMetadata>> measurementCvmArrays;
  for (const auto& conversionMetadataArray : conversionMetadataArrays) {
    std::vector<MeasurementConversionMetadata> measurementCvmArray;
    for (const auto& conversionMetadata : conversionMetadataArray) {
      measurementCvmArray.push_back(
          MeasurementConversionMetadata{conversionMetadata.conv_value});
    }
    measurementCvmArrays.push_back(measurementCvmArray);
  }

  return measurementCvmArrays;
}

// we will receive a list of aggregation formats on publisher side, sharing the
// formats with partner.
template <int MY_ROLE>
const std::vector<AggregationFormat> shareAggregationFormats(
    const std::vector<AggregationFormat>& aggregationFormats) {
  int64_t numAggregationFormats =
      emp::Integer{
          INT_SIZE, static_cast<int64_t>(aggregationFormats.size()), PUBLISHER}
          .reveal<int64_t>();
  XLOGF(DBG, "Shared number of aggregation formats: {}", numAggregationFormats);

  std::vector<int64_t> aggregationIds;
  if constexpr (MY_ROLE == PUBLISHER) {
    for (auto i = 0; i < aggregationFormats.size(); i++) {
      aggregationIds.push_back(aggregationFormats[i].id);
    }
    XLOGF(
        DBG,
        "Sending aggregation format ids: {}",
        private_measurement::vecToString(aggregationIds));
  }

  const auto action = MY_ROLE == PUBLISHER ? "sending" : "receiving";
  XLOGF(DBG, "{} aggregation formats", action);
  auto sharedAggregationFormatIds =
      private_measurement::secret_sharing::privatelyShareIntsFromAlice<MY_ROLE>(
          aggregationIds, numAggregationFormats);
  std::vector<AggregationFormat> out;
  for (auto sharedId : sharedAggregationFormatIds) {
    auto aggregationFormat =
        getAggregationFormatFromIdOrThrow(sharedId.template reveal<int64_t>());
    XLOGF(DBG, "Found aggregation format: {}", aggregationFormat.name);
    out.push_back(aggregationFormat);
  }

  return out;
}

// ad Ids will be used as keys for aggregation in Measurement aggregator.
// sharing the Ids with partner.
template <int MY_ROLE>
const std::vector<int64_t> shareValidAdIds(
    const std::vector<std::vector<TouchpointMetadata>>& tpmArrays) {
  // Compute and then send over the integer ad ids.
  std::vector<int64_t> adIds;
  int64_t numValidAdIds = 0;
  if (MY_ROLE == aggregation::private_aggregation::PUBLISHER) {
    XLOG(DBG, "Computing valid ad ids for sending to partner");
    std::unordered_set<int64_t> adIdSet;
    for (const auto& tmpArray : tpmArrays) {
      for (const auto& tpm : tmpArray) {
        adIdSet.insert(tpm.adId);
      }
    }
    adIds.insert(adIds.end(), adIdSet.begin(), adIdSet.end());
    numValidAdIds = adIdSet.size();
  }

  const emp::Integer empNumValidAdIds{INT_SIZE, numValidAdIds, PUBLISHER};
  numValidAdIds = empNumValidAdIds.reveal<int64_t>();
  XLOGF(INFO, "Number of Ad Ids: {}", numValidAdIds);

  // Send over and then reveal the ad ids
  const auto empAdIds =
      private_measurement::secret_sharing::privatelyShareIntsFromAlice<MY_ROLE>(
          adIds, numValidAdIds, INT_SIZE);
  const auto revealedAdIds =
      private_measurement::secret_sharing::map<emp::Integer, int64_t>(
          empAdIds, [](auto adId) { return adId.template reveal<int64_t>(); });

  XLOGF(
      INFO,
      "Ad Ids to Be Considered: {}",
      private_measurement::vecToString(revealedAdIds));
  return revealedAdIds;
}

// we will initially parse input metrics for all aggregators combined, in this
// function extracting the fields needed for measurement aggregator.
template <int MY_ROLE>
std::pair<MeasurementTpmArrays, MeasurementCvmArrays>
populateMetricsForAdObjectFormat(
    const AggregationInputMetrics& inputData,
    const uint32_t numIds) {
  // Get touchpoint metadata for ad object
  const auto& measurementTpmArrays =
      populateMeasurementTouchpointMetadata(inputData.getTouchpointMetadata());

  XLOG(INFO, "Privately sharing touchpoints...");
  const auto privateTpmArrays = privatelyShareTouchpoints<
      MY_ROLE,
      MeasurementTouchpointMedata,
      PrivateMeasurementTouchpointMetadata>(measurementTpmArrays, numIds);

  const auto measurementCvmArrays =
      populateMeasurementConversionMetadata(inputData.getConversionMetadata());

  XLOG(INFO, "Privately sharing conversions...");
  const auto privateCvmArrays = privatelyShareConversions<
      MY_ROLE,
      MeasurementConversionMetadata,
      PrivateMeasurementConversionMetadata>(measurementCvmArrays, numIds);

  return std::pair<MeasurementTpmArrays, MeasurementCvmArrays>(
      privateTpmArrays, privateCvmArrays);
}

template <int MY_ROLE>
AggregationOutputMetrics computeAggregations(
    const AggregationInputMetrics& inputData,
    fbpcf::Visibility outputVisibility) {
  auto ids = inputData.getIds();
  uint32_t numIds = ids.size();
  XLOGF(INFO, "Have {} ids", numIds);

  // Send over all of the data needed for this computation
  XLOG(INFO, "Sharing aggregation formats...");
  const auto aggregationFormats =
      shareAggregationFormats<MY_ROLE>(inputData.getAggregationFormats());

  const auto& adIds =
      shareValidAdIds<MY_ROLE>(inputData.getTouchpointMetadata());

  MeasurementTpmArrays privateTpmArrays;
  MeasurementCvmArrays privateCvmArrays;
  std::vector<std::vector<PrivateAttributionResult>>
      privateTpmSecretSharePerRule;
  std::vector<std::vector<PrivateAttributionResult>>
      privateCvmSecretSharePerRule;
  for (const auto& aggregationFormat : aggregationFormats) {
    switch (aggregationFormat.id) {
      case AGGREGATION_FORMAT::AD_OBJECT_FORMAT:
        const auto& measurmentArrays =
            populateMetricsForAdObjectFormat<MY_ROLE>(inputData, numIds);
        privateTpmArrays = measurmentArrays.first;
        privateCvmArrays = measurmentArrays.second;
        break;
    }
  }

  PrivateAggregationMetrics aggregationMetrics{
      aggregationFormats, AggregationContext{adIds}, outputVisibility};

  AggregationOutputMetrics out;
  const auto& attributionRules = inputData.getAttributionRules();
  const auto& touchpointSecretShares = inputData.getTouchpointSecretShares();
  const auto& conversionSecretShares = inputData.getConversionSecretShares();

  for (int i = 0; i < attributionRules.size(); i++) {
    // share secret shares computed for each attribution Rule
    XLOG(INFO, "Sharing touchpoint attribution results...");
    std::vector<std::vector<AttributionResult>> tpAttributionResultsPerRule;
    std::vector<std::vector<AttributionResult>> cvmAttributionResultsPerRule;
    // We will share attribution results per attribution rule.
    if (MY_ROLE == PUBLISHER) {
      for(const auto& entries : touchpointSecretShares.at(i))
      {
          std::vector<AttributionResult> results;
          for(const auto& entry: entries)
          {
              results.push_back(AttributionResult{
                  entry.isAttributed
              });
          }
          tpAttributionResultsPerRule.push_back(results);
      }
    }

    privateTpmSecretSharePerRule =
        privatelyShareAttributionResultsTouchpoints<MY_ROLE>(
            tpAttributionResultsPerRule, numIds);

    XLOG(INFO, "Sharing conversion attribution results...");
    if (MY_ROLE == PARTNER) {
      for(const auto& entries : conversionSecretShares.at(i))
      {
          std::vector<AttributionResult> results;
          for(const auto& entry: entries)
          {
              results.push_back(AttributionResult{
                  entry.isAttributed
              });
          }
          cvmAttributionResultsPerRule.push_back(results);
      }
    }
    privateCvmSecretSharePerRule =
        privatelyShareAttributionResultsConversions<MY_ROLE>(
            cvmAttributionResultsPerRule, numIds);

    PrivateAggregation privateAggregation;
    privateAggregation.privateTpm = privateTpmArrays;
    privateAggregation.privateCvm = privateCvmArrays;
    privateAggregation.tpAttributionResults = privateTpmSecretSharePerRule;
    privateAggregation.convAttributionResults = privateCvmSecretSharePerRule;
    aggregationMetrics.computeAggregationsPerFormat(privateAggregation);
    out.ruleToMetrics[attributionRules.at(i)] = aggregationMetrics.reveal();
  }

  return out;
}

} // namespace aggregation::private_aggregation
