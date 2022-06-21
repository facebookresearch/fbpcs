/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <algorithm>
#include <exception>

#include "fbpcs/emp_games/common/Constants.h"
#include "fbpcs/emp_games/pcf2_attribution/AttributionGame.h"
#include "fbpcs/emp_games/pcf2_attribution/Constants.h"

namespace pcf2_attribution {

template <
    int schedulerId,
    bool usingBatch,
    common::InputEncryption inputEncryption>
std::vector<typename AttributionGame<schedulerId, usingBatch, inputEncryption>::
                PrivateTouchpointT>
AttributionGame<schedulerId, usingBatch, inputEncryption>::
    privatelyShareTouchpoints(
        const std::vector<TouchpointT<usingBatch>>& touchpoints) {
  if constexpr (usingBatch) {
    return common::privatelyShareArray<
        Touchpoint<usingBatch>,
        PrivateTouchpoint<schedulerId, usingBatch, inputEncryption>>(
        touchpoints);
  } else {
    return common::privatelyShareArrays<
        Touchpoint<usingBatch>,
        PrivateTouchpoint<schedulerId, usingBatch, inputEncryption>>(
        touchpoints);
  }
}

template <
    int schedulerId,
    bool usingBatch,
    common::InputEncryption inputEncryption>
std::vector<typename AttributionGame<schedulerId, usingBatch, inputEncryption>::
                PrivateConversionT>
AttributionGame<schedulerId, usingBatch, inputEncryption>::
    privatelyShareConversions(
        const std::vector<ConversionT<usingBatch>>& conversions) {
  if constexpr (usingBatch) {
    return common::privatelyShareArray<
        Conversion<usingBatch>,
        PrivateConversion<schedulerId, usingBatch, inputEncryption>>(
        conversions);
  } else {
    return common::privatelyShareArrays<
        Conversion<usingBatch>,
        PrivateConversion<schedulerId, usingBatch, inputEncryption>>(
        conversions);
  }
}

template <
    int schedulerId,
    bool usingBatch,
    common::InputEncryption inputEncryption>
std::vector<std::vector<SecTimestampT<schedulerId, usingBatch>>>
AttributionGame<schedulerId, usingBatch, inputEncryption>::
    privatelyShareThresholds(
        const std::vector<TouchpointT<usingBatch>>& touchpoints,
        const std::vector<PrivateTouchpointT>& privateTouchpoints,
        const AttributionRule<schedulerId, usingBatch, inputEncryption>&
            attributionRule,
        size_t batchSize) {
  std::vector<std::vector<SecTimestampT<schedulerId, usingBatch>>> output;

  if constexpr (inputEncryption != common::InputEncryption::Xor) {
    for (size_t i = 0; i < touchpoints.size(); ++i) {
      if constexpr (usingBatch) {
        auto thresholds =
            attributionRule.computeThresholdsPlaintext(touchpoints.at(i));
        output.push_back(std::move(thresholds));
      } else {
        auto touchpointRow = touchpoints.at(i);
        std::vector<std::vector<SecTimestamp<schedulerId, false>>> thresholdRow;
        for (size_t j = 0; j < touchpointRow.size(); ++j) {
          auto thresholds =
              attributionRule.computeThresholdsPlaintext(touchpointRow.at(j));
          thresholdRow.push_back(std::move(thresholds));
        }
        output.push_back(std::move(thresholdRow));
      }
    }
  } else {
    if constexpr (usingBatch) {
      if (batchSize == 0) {
        throw std::invalid_argument(
            "Must provide positive batch size for batch execution!");
      }
      auto privateIsClick = common::privatelyShareArray<
          Touchpoint<usingBatch>,
          PrivateIsClick<schedulerId, usingBatch, inputEncryption>>(
          touchpoints);
      for (size_t i = 0; i < touchpoints.size(); ++i) {
        auto thresholds = attributionRule.computeThresholdsPrivate(
            privateTouchpoints.at(i), privateIsClick.at(i), batchSize);
        output.push_back(std::move(thresholds));
      }
    } else {
      auto privateIsClick = common::privatelyShareArrays<
          Touchpoint<usingBatch>,
          PrivateIsClick<schedulerId, usingBatch, inputEncryption>>(
          touchpoints);
      for (size_t i = 0; i < privateTouchpoints.size(); ++i) {
        std::vector<std::vector<SecTimestamp<schedulerId, usingBatch>>>
            thresholdRow;
        for (size_t j = 0; j < privateTouchpoints.at(i).size(); ++j) {
          auto thresholds = attributionRule.computeThresholdsPrivate(
              privateTouchpoints.at(i).at(j),
              privateIsClick.at(i).at(j),
              batchSize);
          thresholdRow.push_back(std::move(thresholds));
        }
        output.push_back(std::move(thresholdRow));
      }
    }
  }
  return output;
}

template <
    int schedulerId,
    bool usingBatch,
    common::InputEncryption inputEncryption>
std::vector<std::shared_ptr<
    const AttributionRule<schedulerId, usingBatch, inputEncryption>>>
AttributionGame<schedulerId, usingBatch, inputEncryption>::
    shareAttributionRules(
        const int myRole,
        const std::vector<std::string>& attributionRuleNames) {
  // Publisher converts attribution rule names to attribution rules and ids
  std::vector<std::shared_ptr<
      const AttributionRule<schedulerId, usingBatch, inputEncryption>>>
      attributionRules;
  std::vector<uint64_t> attributionRuleIds;
  if (myRole == common::PUBLISHER) {
    for (auto attributionRuleName : attributionRuleNames) {
      auto attributionRule =
          AttributionRule<schedulerId, usingBatch, inputEncryption>::
              fromNameOrThrow(attributionRuleName);
      attributionRules.push_back(attributionRule);
      attributionRuleIds.push_back(attributionRule->id);
    }
  }

  const size_t attributionRuleIdWidth = 3; // currently we only support 4 rules
  CHECK_LT(
      (SUPPORTED_ATTRIBUTION_RULES<schedulerId, usingBatch, inputEncryption>)
          .size(),
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
          AttributionRule<schedulerId, usingBatch, inputEncryption>::
              fromIdOrThrow(sharedId));
    }
  }
  return attributionRules;
}

template <
    int schedulerId,
    bool usingBatch,
    common::InputEncryption inputEncryption>
const std::vector<SecBit<schedulerId, usingBatch>>
AttributionGame<schedulerId, usingBatch, inputEncryption>::
    computeAttributionsHelper(
        const std::vector<
            PrivateTouchpoint<schedulerId, usingBatch, inputEncryption>>&
            touchpoints,
        const std::vector<
            PrivateConversion<schedulerId, usingBatch, inputEncryption>>&
            conversions,
        const AttributionRule<schedulerId, usingBatch, inputEncryption>&
            attributionRule,
        const std::vector<std::vector<SecTimestamp<schedulerId, usingBatch>>>&
            thresholds,
        size_t batchSize) {
  if constexpr (usingBatch) {
    if (batchSize == 0) {
      throw std::invalid_argument(
          "Must provide positive batch size for batch execution!");
    }
  }
  std::vector<SecBit<schedulerId, usingBatch>> attributions;
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

    if constexpr (usingBatch) {
      OMNISCIENT_ONLY_XLOGF(
          DBG,
          "Computing attributions for conversions: {}",
          common::vecToString(
              conv.ts.openToParty(common::PUBLISHER).getValue()));
    } else {
      OMNISCIENT_ONLY_XLOGF(
          DBG,
          "Computing attributions for conversion: {}",
          conv.ts.openToParty(common::PUBLISHER).getValue());
    }

    // store if conversion has already been attributed
    SecBit<schedulerId, usingBatch> hasAttributedTouchpoint;
    if constexpr (usingBatch) {
      hasAttributedTouchpoint = SecBit<schedulerId, usingBatch>{
          std::vector<bool>(batchSize, false), common::PUBLISHER};
    } else {
      hasAttributedTouchpoint =
          SecBit<schedulerId, usingBatch>{false, common::PUBLISHER};
    }

    CHECK_EQ(touchpoints.size(), thresholds.size())
        << "touchpoints and thresholds are not the same length.";

    for (size_t i = touchpoints.size(); i >= 1; --i) {
      auto tp = touchpoints.at(i - 1);
      auto threshold = thresholds.at(i - 1);

      if constexpr (usingBatch) {
        OMNISCIENT_ONLY_XLOGF(
            DBG,
            "Checking touchpoints: {}",
            common::vecToString(
                tp.ts.openToParty(common::PUBLISHER).getValue()));
      } else {
        OMNISCIENT_ONLY_XLOGF(
            DBG,
            "Checking touchpoint: {}",
            tp.ts.openToParty(common::PUBLISHER).getValue());
      }

      auto isTouchpointAttributable =
          attributionRule.isAttributable(tp, conv, threshold);

      auto isAttributed = isTouchpointAttributable & !hasAttributedTouchpoint;

      hasAttributedTouchpoint = isAttributed | hasAttributedTouchpoint;

      if constexpr (usingBatch) {
        OMNISCIENT_ONLY_XLOGF(
            DBG,
            "isTouchpointAttributable={}, isAttributed={}, hasAttributedTouchpoint={}",
            common::vecToString(
                isTouchpointAttributable.extractBit().getValue()),
            common::vecToString(isAttributed.extractBit().getValue()),
            common::vecToString(
                hasAttributedTouchpoint.extractBit().getValue()));
      } else {
        OMNISCIENT_ONLY_XLOGF(
            DBG,
            "isTouchpointAttributable={}, isAttributed={}, hasAttributedTouchpoint={}",
            isTouchpointAttributable.extractBit().getValue(),
            isAttributed.extractBit().getValue(),
            hasAttributedTouchpoint.extractBit().getValue());
      }

      attributions.push_back(isAttributed);
    }
  }
  std::reverse(attributions.begin(), attributions.end());
  return attributions;
}

template <
    int schedulerId,
    bool usingBatch,
    common::InputEncryption inputEncryption>
std::vector<uint64_t>
AttributionGame<schedulerId, usingBatch, inputEncryption>::
    retrieveValidOriginalTargetIds(
        int myRole,
        std::vector<TouchpointT<usingBatch>>& touchpoints,
        std::vector<ConversionT<usingBatch>>& conversions) {
  std::unordered_set<uint64_t> targetIdSet;

  auto revealTargetId = [&](const auto& secOriginalTargetId) {
    auto publisherTargetId =
        secOriginalTargetId.openToParty(common::PUBLISHER).getValue();
    auto partnerTargetId =
        secOriginalTargetId.openToParty(common::PARTNER).getValue();
    auto revealedTargetId =
        (myRole == common::PUBLISHER) ? publisherTargetId : partnerTargetId;
    return revealedTargetId;
  };

  auto publisherSideExtractTargetId = [&](const auto& touchpointT) {
    SecOriginalTargetId<schedulerId, usingBatch> secOriginalTargetId;
    if (inputEncryption == common::InputEncryption::Xor) {
      typename SecOriginalTargetId<schedulerId, usingBatch>::ExtractedInt
          extractedTargetId(touchpointT.targetId);
      secOriginalTargetId = SecOriginalTargetId<schedulerId, usingBatch>(
          std::move(extractedTargetId));
    } else {
      secOriginalTargetId = SecOriginalTargetId<schedulerId, usingBatch>(
          touchpointT.targetId, common::PUBLISHER);
    }
    return secOriginalTargetId;
  };

  auto partnerSideExtractTargetId = [&](const auto& conversionT) {
    SecOriginalTargetId<schedulerId, usingBatch> secOriginalTargetId;
    if (inputEncryption == common::InputEncryption::Plaintext) {
      secOriginalTargetId = SecOriginalTargetId<schedulerId, usingBatch>(
          conversionT.targetId, common::PARTNER);
    } else {
      typename SecOriginalTargetId<schedulerId, usingBatch>::ExtractedInt
          extractedTargetId(conversionT.targetId);
      secOriginalTargetId = SecOriginalTargetId<schedulerId, usingBatch>(
          std::move(extractedTargetId));
    }
    return secOriginalTargetId;
  };

  if constexpr (usingBatch) {
    // publisher side
    for (auto& touchpointT : touchpoints) {
      // Reveal target id to publisher and partner
      auto secOriginalTargetId = publisherSideExtractTargetId(touchpointT);
      auto revealedTargetId = revealTargetId(secOriginalTargetId);

      touchpointT.originalTargetId = revealedTargetId;
      for (const auto& tid : revealedTargetId) {
        if (tid > 0) {
          targetIdSet.insert(tid);
        }
      }
    }
    // partner side
    for (auto& conversionT : conversions) {
      auto secOriginalTargetId = partnerSideExtractTargetId(conversionT);
      // Reveal target id to publisher and partner
      auto revealedTargetId = revealTargetId(secOriginalTargetId);

      conversionT.originalTargetId = revealedTargetId;
      for (const auto& tid : revealedTargetId) {
        if (tid > 0) {
          targetIdSet.insert(tid);
        }
      }
    }
  } else {
    // publisher side
    for (auto& touchpointT : touchpoints) {
      for (auto& touchpoint : touchpointT) {
        auto secOriginalTargetId = publisherSideExtractTargetId(touchpoint);
        // Reveal target id to publisher and partner
        auto revealedTargetId = revealTargetId(secOriginalTargetId);

        touchpoint.originalTargetId = revealedTargetId;
        if (revealedTargetId > 0) {
          targetIdSet.insert(revealedTargetId);
        }
      }
    }
    // partner side
    for (auto& conversionT : conversions) {
      for (auto& conversion : conversionT) {
        auto secOriginalTargetId = partnerSideExtractTargetId(conversion);
        // Reveal target id to publisher and partner
        auto revealedTargetId = revealTargetId(secOriginalTargetId);

        conversion.originalTargetId = revealedTargetId;
        if (revealedTargetId > 0) {
          targetIdSet.insert(revealedTargetId);
        }
      }
    }
  }
  XLOGF(INFO, "Number of Distinct Target Ids: {}", targetIdSet.size());
  // Added a check here to make sure that number of target Ids never exceed
  // 65,536 (16 unsigned bit).
  CHECK_LE(targetIdSet.size(), 65536)
      << "Number of target Ids cannot be more than 65,536.";

  std::vector<uint64_t> validOriginalTargetIds(
      targetIdSet.begin(), targetIdSet.end());
  return validOriginalTargetIds;
}

template <
    int schedulerId,
    bool usingBatch,
    common::InputEncryption inputEncryption>
void AttributionGame<schedulerId, usingBatch, inputEncryption>::
    replaceTargetIdWithCompressedTargetId(
        std::vector<TouchpointT<usingBatch>>& touchpoints,
        std::vector<ConversionT<usingBatch>>& conversions,
        std::vector<uint64_t>& validOriginalTargetIds) {
  uint64_t compressedTargetId = 1;
  std::unordered_map<uint64_t, uint64_t> targetIdToCompressedTargetIdMap;

  for (auto targetId : validOriginalTargetIds) {
    targetIdToCompressedTargetIdMap.insert({targetId, compressedTargetId});
    compressedTargetId++;
  }

  auto replaceTargetId = [&](auto& attributionArray) {
    if constexpr (usingBatch) {
      for (auto& attributionArrayT : attributionArray) {
        std::vector<uint64_t> compressedTargetId;
        for (const auto& originalTargetId :
             attributionArrayT.originalTargetId) {
          if (originalTargetId > 0) {
            compressedTargetId.push_back(
                targetIdToCompressedTargetIdMap.at(originalTargetId));
          } else {
            compressedTargetId.push_back(originalTargetId);
          }
        }
        attributionArrayT.targetId = compressedTargetId;
      }
    } else {
      for (auto& attributionArrayT : attributionArray) {
        for (auto& attributionData : attributionArrayT) {
          if (attributionData.originalTargetId > 0) {
            attributionData.targetId = targetIdToCompressedTargetIdMap.at(
                attributionData.originalTargetId);
          } else {
            attributionData.targetId = attributionData.originalTargetId;
          }
        }
      }
    }
  };

  replaceTargetId(touchpoints);
  replaceTargetId(conversions);
}

template <
    int schedulerId,
    bool usingBatch,
    common::InputEncryption inputEncryption>
AttributionOutputMetrics
AttributionGame<schedulerId, usingBatch, inputEncryption>::computeAttributions(
    const int myRole,
    const AttributionInputMetrics<usingBatch, inputEncryption>& inputData) {
  XLOG(INFO, "Running attribution");
  auto ids = inputData.getIds();
  uint32_t numIds = ids.size();
  XLOGF(INFO, "Have {} ids", numIds);

  auto touchpointArrays = inputData.getTouchpointArrays();
  auto conversionArrays = inputData.getConversionArrays();

  auto validOriginalTargetId = retrieveValidOriginalTargetIds(
      myRole, touchpointArrays, conversionArrays);

  replaceTargetIdWithCompressedTargetId(
      touchpointArrays, conversionArrays, validOriginalTargetId);

  // Send over all of the data needed for this computation
  XLOG(INFO, "Privately sharing touchpoints...");
  auto tpArrays = privatelyShareTouchpoints(touchpointArrays);
  XLOG(INFO, "Privately sharing conversions...");
  auto convArrays = privatelyShareConversions(conversionArrays);

  // Currently we only have one attribution output format
  std::string attributionFormat = "default";

  // Compute for all of the given attribution rules
  AttributionMetrics attributionMetrics;
  AttributionOutputMetrics out;

  // Publisher shares attribution rules with partner
  auto attributionRules =
      shareAttributionRules(myRole, inputData.getAttributionRules());

  for (const auto& attributionRule : attributionRules) {
    XLOGF(INFO, "Computing attributions for rule {}", attributionRule->name);

    // Share touchpoint threshold information for computing attributions
    auto thresholdArrays = privatelyShareThresholds(
        touchpointArrays, tpArrays, *attributionRule, numIds);
    CHECK_EQ(thresholdArrays.size(), tpArrays.size())
        << "threshold arrays and touchpoint arrays are not the same length.";

    std::vector<SecBitT<schedulerId, usingBatch>> attributions;

    if constexpr (usingBatch) {
      attributions = computeAttributionsHelper(
          tpArrays, convArrays, *attributionRule, thresholdArrays, numIds);
    } else {
      // Compute row by row if not using batch
      for (size_t i = 0; i < numIds; ++i) {
        auto attributionRow = computeAttributionsHelper(
            tpArrays.at(i),
            convArrays.at(i),
            *attributionRule,
            thresholdArrays.at(i),
            numIds);
        attributions.push_back(std::move(attributionRow));
      }
    }

    AttributionOutput<schedulerId, usingBatch> attributionOutput{
        ids, attributions};

    XLOGF(
        INFO,
        "Retrieving attribution results for rule {}.",
        attributionRule->name);
    attributionMetrics.formatToAttribution[attributionFormat] =
        attributionOutput.reveal();
    out.ruleToMetrics[attributionRule->name] = attributionMetrics;

    XLOGF(
        INFO,
        "Done computing attributions for rule {}.",
        attributionRule->name);
  }
  return out;
}

} // namespace pcf2_attribution
