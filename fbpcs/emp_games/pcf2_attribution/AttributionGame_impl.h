/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <fbpcf/io/api/FileIOWrappers.h>
#include <algorithm>
#include <exception>
#include "fbpcs/emp_games/pcf2_attribution/AttributionGame.h"
#include "fbpcs/emp_games/pcf2_attribution/AttributionOptions.h"
#include "fbpcs/emp_games/pcf2_attribution/Constants.h"

namespace pcf2_attribution {

template <int schedulerId>
std::vector<typename AttributionGame<schedulerId>::PrivateTouchpointT>
AttributionGame<schedulerId>::privatelyShareTouchpoints(
    const std::vector<Touchpoint>& touchpoints,
    common::InputEncryption inputEncryption) {
  return common::
      privatelyShareArray<Touchpoint, PrivateTouchpoint<schedulerId>>(
          touchpoints,
          std::bind(
              createPrivateTouchpoint<schedulerId>,
              inputEncryption,
              std::placeholders::_1));
}

template <int schedulerId>
std::vector<typename AttributionGame<schedulerId>::PrivateConversionT>
AttributionGame<schedulerId>::privatelyShareConversions(
    const std::vector<Conversion>& conversions,
    common::InputEncryption inputEncryption) {
  return common::
      privatelyShareArray<Conversion, PrivateConversion<schedulerId>>(
          conversions,
          std::bind(
              createPrivateConversion<schedulerId>,
              inputEncryption,
              std::placeholders::_1));
}

template <int schedulerId>
std::vector<std::vector<SecTimestamp<schedulerId>>>
AttributionGame<schedulerId>::privatelyShareThresholds(
    const std::vector<Touchpoint>& touchpoints,
    const std::vector<PrivateTouchpointT>& privateTouchpoints,
    const AttributionRule<schedulerId>& attributionRule,
    size_t batchSize,
    common::InputEncryption inputEncryption) {
  std::vector<std::vector<SecTimestamp<schedulerId>>> output;

  if (inputEncryption != common::InputEncryption::Xor) {
    for (size_t i = 0; i < touchpoints.size(); ++i) {
      auto thresholds =
          attributionRule.computeThresholdsPlaintext(touchpoints.at(i));
      output.push_back(std::move(thresholds));
    }
  } else {
    if (batchSize == 0) {
      throw std::invalid_argument(
          "Must provide positive batch size for batch execution!");
    }
    auto privateIsClick =
        common::privatelyShareArray<Touchpoint, PrivateIsClick<schedulerId>>(
            touchpoints,
            std::bind(
                createPrivateIsClick<schedulerId>,
                inputEncryption,
                std::placeholders::_1));
    for (size_t i = 0; i < touchpoints.size(); ++i) {
      auto thresholds = attributionRule.computeThresholdsPrivate(
          privateTouchpoints.at(i), privateIsClick.at(i), batchSize);
      output.push_back(std::move(thresholds));
    }
  }
  return output;
}

template <int schedulerId>
std::vector<std::shared_ptr<const AttributionRule<schedulerId>>>
AttributionGame<schedulerId>::shareAttributionRules(
    const int myRole,
    const std::vector<std::string>& attributionRuleNames) {
  // Publisher converts attribution rule names to attribution rules and ids
  std::vector<std::shared_ptr<const AttributionRule<schedulerId>>>
      attributionRules;
  std::vector<uint64_t> attributionRuleIds;
  if (myRole == common::PUBLISHER) {
    for (auto attributionRuleName : attributionRuleNames) {
      auto attributionRule =
          AttributionRule<schedulerId>::fromNameOrThrow(attributionRuleName);
      attributionRules.push_back(attributionRule);
      attributionRuleIds.push_back(attributionRule->id);
    }
  }

  const size_t attributionRuleIdWidth = 3; // currently we only support 4 rules
  CHECK_LT(
      (SUPPORTED_ATTRIBUTION_RULES<schedulerId>).size(),
      (1 << attributionRuleIdWidth));

  // Publisher shares attribution rule ids
  auto sharedAttributionRuleIds = common::privatelyShareIntArrayFrom<
      schedulerId,
      attributionRuleIdWidth,
      common::PUBLISHER,
      common::PARTNER>(myRole, attributionRuleIds);

  if (myRole == common::PARTNER) {
    for (auto sharedId : sharedAttributionRuleIds) {
      attributionRules.push_back(
          AttributionRule<schedulerId>::fromIdOrThrow(sharedId));
    }
  }
  return attributionRules;
}

template <int schedulerId>
const std::vector<uint64_t>
AttributionGame<schedulerId>::retrieveValidOriginalAdIds(
    const int /*myRole*/,
    std::vector<Touchpoint>& touchpoints,
    common::InputEncryption inputEncryption) {
  std::unordered_set<uint64_t> adIdSet;
  for (auto& touchpoint : touchpoints) {
    SecOriginalAdId<schedulerId> secAdId;
    if (inputEncryption == common::InputEncryption::Xor) {
      typename SecOriginalAdId<schedulerId>::ExtractedInt extractedAdIds(
          touchpoint.originalAdId);
      secAdId = SecOriginalAdId<schedulerId>(std::move(extractedAdIds));
      // Reveal ad id to publisher
      auto publisherAdId = secAdId.openToParty(common::PUBLISHER).getValue();
      touchpoint.originalAdId = publisherAdId;
    }
    for (auto& adId : touchpoint.originalAdId) {
      if (adId > 0) {
        adIdSet.insert(adId);
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
void AttributionGame<schedulerId>::replaceAdIdWithCompressedAdId(
    std::vector<Touchpoint>& touchpoints,
    std::vector<uint64_t>& validOriginalAdIds) {
  uint16_t compressedAdId = 1;
  std::unordered_map<uint64_t, uint16_t> adIdToCompressedAdIdMap;

  for (auto adId : validOriginalAdIds) {
    adIdToCompressedAdIdMap.insert({adId, compressedAdId});
    compressedAdId++;
  }

  for (auto& touchpoint : touchpoints) {
    std::vector<uint64_t> adIds;
    uint16_t defaultAdId = 0;
    for (auto& originalAdId : touchpoint.originalAdId) {
      if (originalAdId > 0) {
        adIds.push_back(adIdToCompressedAdIdMap.at(originalAdId));
      } else {
        adIds.push_back(defaultAdId);
      }
    }
    touchpoint.adId = adIds;
  }
}

template <int schedulerId>
void AttributionGame<schedulerId>::putAdIdMappingJson(
    const CompressedAdIdToOriginalAdId& maps,
    std::string outputPath) {
  std::string content = maps.toJson();
  fbpcf::io::FileIOWrappers::writeFile(outputPath, content);
}

template <int schedulerId>
const std::vector<SecBit<schedulerId>>
AttributionGame<schedulerId>::computeAttributionsHelper(
    const std::vector<PrivateTouchpoint<schedulerId>>& touchpoints,
    const std::vector<PrivateConversion<schedulerId>>& conversions,
    const AttributionRule<schedulerId>& attributionRule,
    const std::vector<std::vector<SecTimestamp<schedulerId>>>& thresholds,
    size_t batchSize) {
  if (batchSize == 0) {
    throw std::invalid_argument(
        "Must provide positive batch size for batch execution!");
  }
  std::vector<SecBit<schedulerId>> attributions;
  // We will be attributing on a sorted vector of touchpoints and conversions
  // (based on timestamps).
  // The preferred touchpoint for a conversion will be a valid attributable
  // touchpoint with nearest timestamp to the conversion. In order to compute
  // this efficiently, we will traverse backwards on both conversion and
  // touchpoint vector. So that when we find a valid attributable touchpoint, we
  // know that it is the preferred touchpoint as well.
  // Thus at the end we will get the fully reversed attribution match vector of
  // conversions and touchpoints.
  for (auto conversion = conversions.rbegin(); conversion != conversions.rend();
       ++conversion) {
    auto conv = *conversion;

    OMNISCIENT_ONLY_XLOGF(
        DBG,
        "Computing attributions for conversions: {}",
        common::vecToString(conv.ts.openToParty(common::PUBLISHER).getValue()));

    // store if conversion has already been attributed
    SecBit<schedulerId> hasAttributedTouchpoint;

    hasAttributedTouchpoint = SecBit<schedulerId>{
        std::vector<bool>(batchSize, false), common::PUBLISHER};

    CHECK_EQ(touchpoints.size(), thresholds.size())
        << "touchpoints and thresholds are not the same length.";

    for (size_t i = touchpoints.size(); i >= 1; --i) {
      auto tp = touchpoints.at(i - 1);
      auto threshold = thresholds.at(i - 1);

      OMNISCIENT_ONLY_XLOGF(
          DBG,
          "Checking touchpoints: {}",
          common::vecToString(tp.ts.openToParty(common::PUBLISHER).getValue()));

      auto isTouchpointAttributable =
          attributionRule.isAttributable(tp, conv, threshold);

      auto isAttributed = isTouchpointAttributable & !hasAttributedTouchpoint;

      hasAttributedTouchpoint = isAttributed | hasAttributedTouchpoint;

      OMNISCIENT_ONLY_XLOGF(
          DBG,
          "isTouchpointAttributable={}, isAttributed={}, hasAttributedTouchpoint={}",
          common::vecToString(isTouchpointAttributable.extractBit().getValue()),
          common::vecToString(isAttributed.extractBit().getValue()),
          common::vecToString(hasAttributedTouchpoint.extractBit().getValue()));

      attributions.push_back(isAttributed);
    }
  }
  std::reverse(attributions.begin(), attributions.end());
  return attributions;
}

template <int schedulerId>
const std::vector<AttributionReformattedOutputFmt<schedulerId>>
AttributionGame<schedulerId>::computeAttributionsHelperV2(
    const std::vector<PrivateTouchpoint<schedulerId>>& touchpoints,
    const std::vector<PrivateConversion<schedulerId>>& conversions,
    const AttributionRule<schedulerId>& attributionRule,
    const std::vector<std::vector<SecTimestamp<schedulerId>>>& thresholds,
    size_t batchSize) {
  if (batchSize == 0) {
    throw std::invalid_argument(
        "Must provide positive batch size for batch execution!");
  }
  std::vector<AttributionReformattedOutputFmt<schedulerId>> attributionsOutput;
  // We will be attributing on a sorted vector of touchpoints and conversions
  // (based on timestamps).
  // The preferred touchpoint for a conversion will be a valid attributable
  // touchpoint with nearest timestamp to the conversion. In order to compute
  // this efficiently, we will traverse backwards on both conversion and
  // touchpoint vector. So that when we find a valid attributable touchpoint, we
  // know that it is the preferred touchpoint as well.
  // Thus at the end we will get the fully reversed attribution match vector of
  // conversions and touchpoints.
  for (auto conversion = conversions.rbegin(); conversion != conversions.rend();
       ++conversion) {
    auto conv = *conversion;

    OMNISCIENT_ONLY_XLOGF(
        DBG,
        "Computing attributions for conversions: {}",
        common::vecToString(conv.ts.openToParty(common::PUBLISHER).getValue()));

    // store if conversion has already been attributed
    SecBit<schedulerId> hasAttributedTouchpoint;
    hasAttributedTouchpoint = SecBit<schedulerId>{
        std::vector<bool>(batchSize, false), common::PUBLISHER};

    CHECK_EQ(touchpoints.size(), thresholds.size())
        << "touchpoints and thresholds are not the same length.";

    SecAdId<schedulerId> attributedAdId;
    uint64_t defaultAdId = 0;

    // initialize the ad_id to be 0, is_attributed to be false:
    attributedAdId = SecAdId<schedulerId>{
        std::vector<uint64_t>(batchSize, defaultAdId), common::PUBLISHER};

    for (size_t i = touchpoints.size(); i >= 1; --i) {
      auto tp = touchpoints.at(i - 1);
      auto threshold = thresholds.at(i - 1);

      OMNISCIENT_ONLY_XLOGF(
          DBG,
          "Checking touchpoints: {}",
          common::vecToString(tp.ts.openToParty(common::PUBLISHER).getValue()));

      auto isTouchpointAttributable =
          attributionRule.isAttributable(tp, conv, threshold);

      auto isAttributed = isTouchpointAttributable & !hasAttributedTouchpoint;

      hasAttributedTouchpoint = isAttributed | hasAttributedTouchpoint;

      OMNISCIENT_ONLY_XLOGF(
          DBG,
          "isTouchpointAttributable={}, isAttributed={}, hasAttributedTouchpoint={}",
          common::vecToString(isTouchpointAttributable.extractBit().getValue()),
          common::vecToString(isAttributed.extractBit().getValue()),
          common::vecToString(hasAttributedTouchpoint.extractBit().getValue()));

      attributedAdId = attributedAdId.mux(isAttributed, tp.adId);
    }
    attributionsOutput.push_back(AttributionReformattedOutputFmt<schedulerId>{
        .ad_id = attributedAdId,
        .conv_value = conv.convValue,
        .is_attributed = hasAttributedTouchpoint});
  }
  std::reverse(attributionsOutput.begin(), attributionsOutput.end());
  return attributionsOutput;
}

template <int schedulerId>
AttributionOutputMetrics AttributionGame<schedulerId>::computeAttributions(
    const int myRole,
    const AttributionInputMetrics& inputData,
    common::InputEncryption inputEncryption) {
  auto
      [thresholdArraysForEachRule,
       tpArrays,
       convArrays,
       attributionRules,
       ids] = prepareMpcInputs(myRole, inputData, inputEncryption);

  return computeAttributions_impl(
      thresholdArraysForEachRule, tpArrays, convArrays, attributionRules, ids);
}

template <int schedulerId>
MpcInputs<schedulerId> AttributionGame<schedulerId>::prepareMpcInputs(
    const int myRole,
    const AttributionInputMetrics& inputData,
    common::InputEncryption inputEncryption) {
  XLOG(INFO, "Running attribution");
  auto ids = inputData.getIds();

  // Compress the original ad id when new format is used:
  auto touchpoints = inputData.getTouchpointArrays();
  if (FLAGS_use_new_output_format) {
    XLOG(INFO, "Retrieving original Ad Ids...");
    auto validOriginalAdIds =
        retrieveValidOriginalAdIds(myRole, touchpoints, inputEncryption);
    XLOG(INFO, "Replacing original ad Ids with compressed ad Ids");

    CompressedAdIdToOriginalAdId map;
    uint16_t compressedAdId = 1;
    for (auto originalAdId : validOriginalAdIds) {
      map.compressedAdIdToAdIdMap[std::to_string(compressedAdId)] =
          originalAdId;
      compressedAdId++;
    }
    std::string outputJsonFilename =
        FLAGS_output_base_path + "compressionMapping.json";
    putAdIdMappingJson(map, outputJsonFilename);

    // replace adId with compressed adId:
    replaceAdIdWithCompressedAdId(touchpoints, validOriginalAdIds);
  }
  // Send over all of the data needed for this computation
  XLOG(INFO, "Privately sharing touchpoints...");
  auto tpArrays = privatelyShareTouchpoints(touchpoints, inputEncryption);
  XLOG(INFO, "Privately sharing conversions...");
  auto convArrays = privatelyShareConversions(
      inputData.getConversionArrays(), inputEncryption);

  // Publisher shares attribution rules with partner
  auto attributionRules =
      shareAttributionRules(myRole, inputData.getAttributionRules());

  std::vector<std::vector<std::vector<SecTimestamp<schedulerId>>>>
      thresholdArraysForEachRule;
  thresholdArraysForEachRule.reserve(attributionRules.size());

  for (const auto& attributionRule : attributionRules) {
    XLOGF(INFO, "Computing thresholds for rule {}", attributionRule->name);
    thresholdArraysForEachRule.push_back(privatelyShareThresholds(
        touchpoints, tpArrays, *attributionRule, ids.size(), inputEncryption));
    CHECK_EQ(thresholdArraysForEachRule.back().size(), tpArrays.size())
        << "threshold arrays and touchpoint arrays are not the same length.";
  }
  return {
      thresholdArraysForEachRule, tpArrays, convArrays, attributionRules, ids};
}

template <int schedulerId>
AttributionOutputMetrics AttributionGame<schedulerId>::computeAttributions_impl(
    std::vector<std::vector<std::vector<SecTimestamp<schedulerId>>>>&
        thresholdArraysForEachRule,
    std::vector<typename AttributionGame<schedulerId>::PrivateTouchpointT>&
        tpArrays,
    std::vector<typename AttributionGame<schedulerId>::PrivateConversionT>&
        convArrays,
    std::vector<std::shared_ptr<const AttributionRule<schedulerId>>>&
        attributionRules,
    std::vector<int64_t>& ids) {
  auto numIds = ids.size();
  XLOGF(INFO, "Have {} ids", numIds);
  // Currently we only have one attribution output format
  std::string attributionFormat = "default";

  // Compute for all of the given attribution rules
  AttributionMetrics attributionMetrics;
  AttributionOutputMetrics out;
  for (size_t i = 0; i < attributionRules.size(); i++) {
    auto& attributionRule = attributionRules.at(i);
    XLOGF(INFO, "Computing attributions for rule {}", attributionRule->name);

    // Share touchpoint threshold information for computing attributions
    auto& thresholdArrays = thresholdArraysForEachRule.at(i);

    if (FLAGS_use_new_output_format) {
      std::vector<AttributionReformattedOutputFmtT<schedulerId>>
          attributionsReformatted;

      attributionsReformatted = computeAttributionsHelperV2(
          tpArrays, convArrays, *attributionRule, thresholdArrays, numIds);

      AttributionReformattedOutput<schedulerId> attributionReformattedOutput{
          ids, attributionsReformatted};
      XLOGF(
          INFO,
          "Retrieving attribution results for rule {}.",
          attributionRule->name);
      attributionMetrics.attributionResult =
          attributionReformattedOutput.reveal();

    } else {
      std::vector<SecBit<schedulerId>> attributions;

      attributions = computeAttributionsHelper(
          tpArrays, convArrays, *attributionRule, thresholdArrays, numIds);

      AttributionOutput<schedulerId> attributionOutput{ids, attributions};

      XLOGF(
          INFO,
          "Retrieving attribution results for rule {}.",
          attributionRule->name);
      attributionMetrics.formatToAttribution[attributionFormat] =
          attributionOutput.reveal();
    }

    out.ruleToMetrics[attributionRule->name] = attributionMetrics;
    XLOGF(
        INFO,
        "Done computing attributions for rule {}.",
        attributionRule->name);
  }
  return out;
}

} // namespace pcf2_attribution
