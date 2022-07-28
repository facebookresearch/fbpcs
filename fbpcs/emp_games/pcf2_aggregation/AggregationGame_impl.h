/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <fbpcs/emp_games/pcf2_aggregation/AggregationOptions.h>
#include <fbpcs/emp_games/pcf2_aggregation/AttributionReformattedResult.h>
#include "fbpcf/engine/util/AesPrgFactory.h"
#include "fbpcf/mpc_std_lib/oram/DifferenceCalculatorFactory.h"
#include "fbpcf/mpc_std_lib/oram/LinearOramFactory.h"
#include "fbpcf/mpc_std_lib/oram/ObliviousDeltaCalculatorFactory.h"
#include "fbpcf/mpc_std_lib/oram/SinglePointArrayGeneratorFactory.h"
#include "fbpcf/mpc_std_lib/oram/WriteOnlyOramFactory.h"
#include "folly/logging/xlog.h"

namespace pcf2_aggregation {

template <int schedulerId>
std::vector<std::vector<PrivateMeasurementTouchpointMetadata<schedulerId>>>
AggregationGame<schedulerId>::privatelyShareMeasurementTouchpointMetadata(
    const std::vector<std::vector<TouchpointMetadata>>& touchpointMetadata) {
  return common::privatelyShareArrays<
      TouchpointMetadata,
      PrivateMeasurementTouchpointMetadata<schedulerId>>(touchpointMetadata);
}

template <int schedulerId>
std::vector<std::vector<PrivateMeasurementConversionMetadata<schedulerId>>>
AggregationGame<schedulerId>::privatelyShareMeasurementConversionMetadata(
    const std::vector<std::vector<ConversionMetadata>>& conversionMetadata) {
  return common::privatelyShareArrays<
      ConversionMetadata,
      PrivateMeasurementConversionMetadata<schedulerId>>(conversionMetadata);
}

template <int schedulerId>
std::vector<std::vector<PrivateAttributionResult<schedulerId>>>
AggregationGame<schedulerId>::privatelyShareAttributionResults(
    const std::vector<std::vector<AttributionResult>>& attributionResults) {
  return common::privatelyShareArrays<
      AttributionResult,
      PrivateAttributionResult<schedulerId>>(attributionResults);
}

template <int schedulerId>
std::vector<std::vector<PrivateAttributionReformattedResult<schedulerId>>>
AggregationGame<schedulerId>::privatelyShareAttributionReformattedResults(
    const std::vector<std::vector<AttributionReformattedResult>>&
        attributionReformattedResults) {
  return common::privatelyShareArrays<
      AttributionReformattedResult,
      PrivateAttributionReformattedResult<schedulerId>>(
      attributionReformattedResults);
}

template <int schedulerId>
const std::vector<uint64_t>
AggregationGame<schedulerId>::retrieveValidOriginalAdIds(
    const int myRole,
    std::vector<std::vector<TouchpointMetadata>>& touchpointMetadataArrays) {
  std::unordered_set<uint64_t> adIdSet;
  for (auto& touchpointMetadataArray : touchpointMetadataArrays) {
    for (auto& touchpointMetadata : touchpointMetadataArray) {
      // Share ad id
      SecOriginalAdId<schedulerId> secAdId;
      if (inputEncryption_ == common::InputEncryption::Xor) {
        typename SecOriginalAdId<schedulerId>::ExtractedInt extractedAdId(
            touchpointMetadata.originalAdId);
        secAdId = SecOriginalAdId<schedulerId>(std::move(extractedAdId));
      } else {
        secAdId = SecOriginalAdId<schedulerId>(
            touchpointMetadata.originalAdId, common::PUBLISHER);
      }

      // Reveal ad id to publisher and partner
      auto publisherAdId = secAdId.openToParty(common::PUBLISHER).getValue();
      auto partnerAdId = secAdId.openToParty(common::PARTNER).getValue();
      auto revealedAdId =
          (myRole == common::PUBLISHER) ? publisherAdId : partnerAdId;

      touchpointMetadata.originalAdId = revealedAdId;
      if (revealedAdId > 0) {
        adIdSet.insert(revealedAdId);
      }
    }
  }

  XLOGF(INFO, "Number of Ad Ids: {}", adIdSet.size());
  // Added a check here to make sure that number of ad Ids never exceed 65,536
  // (8 unsigned bit)
  CHECK_LE(adIdSet.size(), 65536)
      << "Number of ad Ids cannot be more than 65,536.";

  std::vector<uint64_t> validOriginalAdIds;
  validOriginalAdIds.insert(
      validOriginalAdIds.end(), adIdSet.begin(), adIdSet.end());
  std::sort(validOriginalAdIds.begin(), validOriginalAdIds.end());
  return validOriginalAdIds;
}

template <int schedulerId>
void AggregationGame<schedulerId>::replaceAdIdWithCompressedAdId(
    std::vector<std::vector<TouchpointMetadata>>& touchpointMetadataArrays,
    std::vector<uint64_t>& validOriginalAdIds) {
  uint16_t compressedAdId = 1;
  std::unordered_map<uint64_t, uint16_t> adIdToCompressedAdIdMap;

  for (auto adId : validOriginalAdIds) {
    adIdToCompressedAdIdMap.insert({adId, compressedAdId});
    compressedAdId++;
  }

  for (auto& touchpointMetadataArray : touchpointMetadataArrays) {
    for (auto& touchpointMetadata : touchpointMetadataArray) {
      if (touchpointMetadata.originalAdId > 0) {
        touchpointMetadata.adId =
            adIdToCompressedAdIdMap.at(touchpointMetadata.originalAdId);
      }
    }
  }
}

template <int schedulerId>
const std::vector<AggregationFormat<schedulerId>>
AggregationGame<schedulerId>::shareAggregationFormats(
    const int myRole,
    const std::vector<std::string>& aggregationFormatNames) {
  std::vector<AggregationFormat<schedulerId>> aggregationFormats;
  std::vector<uint64_t> aggregationFormatIds;

  // Publisher converts aggregation format names to aggregation formats and
  // ids
  if (myRole == common::PUBLISHER) {
    for (auto aggregationFormatName : aggregationFormatNames) {
      auto aggregationFormat = AggregationFormat<schedulerId>::fromNameOrThrow(
          aggregationFormatName);
      aggregationFormats.push_back(aggregationFormat);
      aggregationFormatIds.push_back(aggregationFormat.id);
    }
  }

  const size_t aggregationFormatIdWidth = 1; // currently we support 1 format
  CHECK_LT(
      (SUPPORTED_AGGREGATION_FORMATS<schedulerId>).size(),
      (1 << aggregationFormatIdWidth));

  // Publisher shares aggregation format ids
  auto sharedAggregationFormatIds = common::privatelyShareIntArrayFrom<
      schedulerId,
      aggregationFormatIdWidth,
      common::PUBLISHER,
      common::PARTNER>(myRole, aggregationFormatIds);

  if (myRole == common::PARTNER) {
    for (auto sharedAggregationFormatId : sharedAggregationFormatIds) {
      aggregationFormats.push_back(
          AggregationFormat<schedulerId>::fromIdOrThrow(
              sharedAggregationFormatId));
    }
  }
  return aggregationFormats;
}

template <int schedulerId>
AggregationOutputMetrics AggregationGame<schedulerId>::computeAggregations(
    const int myRole,
    const AggregationInputMetrics& inputData) {
  XLOG(INFO, "Running private aggregation");

  auto ids = inputData.getIds();
  uint32_t numIds = ids.size();
  XLOGF(INFO, "Have {} ids", numIds);

  // Send over all of the data needed for this computation
  XLOG(INFO, "Sharing aggregation formats...");
  const auto aggregationFormats =
      shareAggregationFormats(myRole, inputData.getAggregationFormats());
  auto touchpointMetadataArrays = inputData.getTouchpointMetadata();

  XLOG(INFO, "Sharing original Ad Ids...");
  auto validOriginalAdIds =
      retrieveValidOriginalAdIds(myRole, touchpointMetadataArrays);

  XLOG(INFO, "Replacing original ad Ids with compressed ad Ids");
  replaceAdIdWithCompressedAdId(touchpointMetadataArrays, validOriginalAdIds);

  XLOG(INFO, "Sharing touchpoint and conversion metadata...");
  MeasurementTpmArrays<schedulerId> privateTpmArrays;
  MeasurementCvmArrays<schedulerId> privateCvmArrays;
  std::vector<std::vector<PrivateAttributionResult<schedulerId>>>
      privateAttributionResult;
  std::vector<std::vector<PrivateAttributionReformattedResult<schedulerId>>>
      privateAttributionReformattedResults;

  for (const auto& aggregationFormat : aggregationFormats) {
    switch (aggregationFormat.id) {
      case AGGREGATION_FORMAT::AD_OBJECT_FORMAT:
        privateTpmArrays = AggregationGame<schedulerId>::
            privatelyShareMeasurementTouchpointMetadata(
                touchpointMetadataArrays);
        privateCvmArrays = AggregationGame<schedulerId>::
            privatelyShareMeasurementConversionMetadata(
                inputData.getConversionMetadata());
        break;
    }
  }

  const int8_t indicatorSumWidth = adIdWidth;
  bool isPublisher = (myRole == common::PUBLISHER);
  auto oramRole = isPublisher
      ? fbpcf::mpc_std_lib::oram::IWriteOnlyOram<
            fbpcf::mpc_std_lib::util::AggregationValue>::Alice
      : fbpcf::mpc_std_lib::oram::IWriteOnlyOram<
            fbpcf::mpc_std_lib::util::AggregationValue>::Bob;

  PrivateAggregationMetrics<schedulerId> aggregationMetrics{
      aggregationFormats,
      AggregationContext{validOriginalAdIds},
      myRole,
      concurrency_,
      // linear ORAM will be less efficient theoretically if ORAM size is larger
      // than 4. Since ORAM size is adid size + 1, we use 3 as the threshold
      // here.
      std::move(
          validOriginalAdIds.size() > 3
              ? fbpcf::mpc_std_lib::oram::getSecureWriteOnlyOramFactory<
                    fbpcf::mpc_std_lib::util::AggregationValue,
                    indicatorSumWidth,
                    schedulerId>(isPublisher, 0, 1, *communicationAgentFactory_)
              : fbpcf::mpc_std_lib::oram::getSecureLinearOramFactory<
                    fbpcf::mpc_std_lib::util::AggregationValue,
                    schedulerId>(
                    isPublisher, 0, 1, *communicationAgentFactory_))};

  AggregationOutputMetrics out;
  const auto& attributionRules = inputData.getAttributionRules();

  if (FLAGS_use_new_output_format) {
    const auto& attributionReformattedSecretShares =
        inputData.getAttributionReformattedSecretShares();
    for (size_t i = 0; i < attributionRules.size(); ++i) {
      // share secret shares computed for each attribution Rule
      std::vector<std::vector<AttributionReformattedResult>>
          attributionReformattedResultsPerRule;
      // We will share attribution results per attribution rule.
      for (const auto& entries : attributionReformattedSecretShares.at(i)) {
        std::vector<AttributionReformattedResult> results;
        for (const auto& entry : entries) {
          results.push_back(AttributionReformattedResult{
              entry.adId, entry.convValue, entry.isAttributed});
        }
        attributionReformattedResultsPerRule.push_back(results);
      }

      XLOG(INFO, "Sharing reformatted attribution results...");
      auto secretReformattedSharePerRule = AggregationGame<schedulerId>::
          privatelyShareAttributionReformattedResults(
              attributionReformattedResultsPerRule);

      PrivateAggregationReformatted<schedulerId> privateAggregationReformatted{
          secretReformattedSharePerRule};

      aggregationMetrics.computeAggregationsReformattedPerFormat(
          privateAggregationReformatted);

      // currently we only support one aggregation format
      XLOGF(
          INFO,
          "Done computing aggregation for {} and {}.",
          aggregationFormats.at(0).name,
          attributionRules.at(i));

      out.ruleToMetrics[attributionRules.at(i)] = aggregationMetrics.reveal();
    }
  } else {
    const auto& attributionSecretShares =
        inputData.getAttributionSecretShares();

    for (size_t i = 0; i < attributionRules.size(); ++i) {
      // share secret shares computed for each attribution Rule
      std::vector<std::vector<AttributionResult>> attributionResultsPerRule;
      // We will share attribution results per attribution rule.
      for (const auto& entries : attributionSecretShares.at(i)) {
        std::vector<AttributionResult> results;
        for (const auto& entry : entries) {
          results.push_back(AttributionResult{entry.isAttributed});
        }
        attributionResultsPerRule.push_back(results);
      }

      XLOG(INFO, "Sharing attribution results...");
      auto secretSharePerRule =
          AggregationGame<schedulerId>::privatelyShareAttributionResults(
              attributionResultsPerRule);

      PrivateAggregation<schedulerId> privateAggregation{
          secretSharePerRule, privateTpmArrays, privateCvmArrays};

      aggregationMetrics.computeAggregationsPerFormat(privateAggregation);

      // currently we only support one aggregation format
      XLOGF(
          INFO,
          "Done computing aggregation for {} and {}.",
          aggregationFormats.at(0).name,
          attributionRules.at(i));

      out.ruleToMetrics[attributionRules.at(i)] = aggregationMetrics.reveal();
    }
  }
  return out;
}
} // namespace pcf2_aggregation
