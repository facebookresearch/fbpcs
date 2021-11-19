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

#include "fbpcs/emp_games/attribution/decoupled_attribution/AttributionMetrics.h"
#include "fbpcs/emp_games/attribution/decoupled_attribution/AttributionOptions.h"
#include "fbpcs/emp_games/attribution/decoupled_attribution/Timestamp.h"

namespace aggregation::private_attribution {

static const Touchpoint TOUCHPOINT_PADDING_VALUE{
    /* id */ INVALID_TP_ID,
    /* isClick */ false,
    /* ts */ -1};

static const Conversion CONVERSION_PADDING_VALUE{/* ts */ -1};

template <int MY_ROLE>
const auto privatelyShareTouchpoints = std::bind(
    private_measurement::secret_sharing::
        privatelyShareArraysFromAlice<MY_ROLE, Touchpoint, PrivateTouchpoint>,
    std::placeholders::_1,
    std::placeholders::_2,
    FLAGS_max_num_touchpoints,
    TOUCHPOINT_PADDING_VALUE);

template <int MY_ROLE>
const auto privatelyShareConversions = std::bind(
    private_measurement::secret_sharing::
        privatelyShareArraysFromBob<MY_ROLE, Conversion, PrivateConversion>,
    std::placeholders::_1,
    std::placeholders::_2,
    FLAGS_max_num_conversions,
    CONVERSION_PADDING_VALUE);

template <int MY_ROLE>
const std::vector<AttributionRule> shareAttributionRules(
    const std::vector<AttributionRule>& rules) {
  int64_t numAttributionRules =
      emp::Integer{INT_SIZE, static_cast<int64_t>(rules.size()), PUBLISHER}
          .reveal<int64_t>();
  XLOGF(DBG, "Shared number of attribution rules: {}", numAttributionRules);

  std::vector<int64_t> attributionIds;
  if constexpr (MY_ROLE == PUBLISHER) {
    for (std::vector<AttributionRule>::size_type i = 0; i < rules.size(); i++) {
      attributionIds.push_back(rules[i].id);
    }
    XLOGF(
        DBG,
        "Sending attribution rule ids: {}",
        private_measurement::vecToString(attributionIds));
  }

  const auto action = MY_ROLE == PUBLISHER ? "sending" : "receiving";
  XLOGF(DBG, "{} attribution rules", action);
  auto sharedAttributionIds =
      private_measurement::secret_sharing::privatelyShareIntsFromAlice<MY_ROLE>(
          attributionIds, numAttributionRules);
  std::vector<AttributionRule> out;
  for (auto sharedId : sharedAttributionIds) {
    auto rule =
        AttributionRule::fromIdOrThrow(sharedId.template reveal<int64_t>());
    XLOGF(DBG, "Found rule: {}", rule.name);
    out.push_back(rule);
  }

  return out;
}

/**
 * Computes the ad attributions for the given id.
 */
const std::vector<PrivateAttribution> computeAttributionsForId(
    const int64_t id,
    const std::vector<PrivateTouchpoint>& touchpoints,
    const std::vector<PrivateConversion>& conversions,
    const AttributionRule& attributionRule) {
  std::vector<PrivateAttribution> attributions;
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
        DBG, "Computing attributions for conversion: {}", conv.reveal());
    emp::Bit hasAttributedTouchpoint = emp::Bit{false};
    for (auto touchpoint = touchpoints.rbegin();
         touchpoint != touchpoints.rend();
         ++touchpoint) {
      auto tp = *touchpoint;
      OMNISCIENT_ONLY_XLOGF(
          DBG, "Checking touchpoint: {}", tp.reveal(emp::PUBLIC));

      // Start with an unattributed attribution
      PrivateAttribution attribution{
          /* uid */ id,
          /* hasAttributedTouchpoint */ emp::Bit{false},
          /* conv */ conv,
          /* tp */ tp};

      // Only use the touchpoint if it's valid (not padding), attributable
      auto isTouchpointAttributable = attributionRule.isAttributable(tp, conv);
      auto isTouchpointValid = tp.isValid();

      attribution.hasAttributedTouchpoint = isTouchpointAttributable &
          isTouchpointValid & !hasAttributedTouchpoint;

      OMNISCIENT_ONLY_XLOGF(
          DBG,
          "isTouchpointAttributable={}, isTouchpointValid={}",
          isTouchpointAttributable.reveal<bool>(),
          isTouchpointValid.reveal<bool>());
      hasAttributedTouchpoint = emp::If(
          hasAttributedTouchpoint,
          hasAttributedTouchpoint,
          isTouchpointAttributable & isTouchpointValid);
      attributions.push_back(attribution);
    }
  }

  return attributions;
}

template <int MY_ROLE>
AttributionOutputMetrics computeAttributions(
    const AttributionInputMetrics& inputData,
    fbpcf::Visibility outputVisibility) {
  auto ids = inputData.getIds();
  uint32_t numIds = ids.size();
  XLOGF(INFO, "Have {} ids", numIds);

  // Send over all of the data needed for this computation
  XLOG(INFO, "Sharing attribution rules...");
  const auto attributionRules =
      shareAttributionRules<MY_ROLE>(inputData.getAttributionRules());
  XLOG(INFO, "Privately sharing touchpoints...");
  const auto tpArrays = privatelyShareTouchpoints<MY_ROLE>(
      inputData.getTouchpointArrays(), numIds);
  XLOG(INFO, "Privately sharing conversions...");
  const auto convArrays = privatelyShareConversions<MY_ROLE>(
      inputData.getConversionArrays(), numIds);

  // Compute for all of the given attribution rules
  AttributionOutputMetrics out;
  for (const auto attributionRule : attributionRules) {
    XLOGF(INFO, "Computing attributions for rule {}", attributionRule.name);

    // Currently we have one attribution output format
    std::vector<AttributionFormat> attributionFormats;
    attributionFormats.push_back(
        getAttributionFormatFromNameOrThrow("default"));

    // Compute all attributions for all rule/format combinations.
    PrivateAttributionMetrics attributionMetrics{
        attributionRule,
        attributionFormats,
        AttributionContext{inputData.getIds(), tpArrays},
        outputVisibility};
    for (std::vector<int64_t>::size_type i = 0; i < numIds; i++) {
      auto id = ids[i];
      auto tps = tpArrays[i];
      auto convs = convArrays[i];

      XLOGF(
          DBG,
          "Processing ID {}\nClicks: {}\nConversions: {}",
          id,
          private_measurement::privateVecToString<MY_ROLE, PUBLISHER>(tps),
          private_measurement::privateVecToString<MY_ROLE, PARTNER>(convs));

      auto attributionsForId =
          computeAttributionsForId(id, tps, convs, attributionRule);
      for (auto attribution = attributionsForId.rbegin();
           attribution != attributionsForId.rend();
           ++attribution) {
        attributionMetrics.addAttribution((*attribution));
      }
    }

    XLOGF(
        DBG,
        "Revealing aggregated attribution results for {} to both parties.",
        attributionRule.name);
    out.ruleToMetrics[attributionRule.name] = attributionMetrics.reveal();
  }

  return out;
}
} // namespace aggregation::private_attribution
