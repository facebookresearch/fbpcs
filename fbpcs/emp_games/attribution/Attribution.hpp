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

#include "../common/EmpOperationUtil.h"
#include "../common/PrivateData.h"
#include "../common/SecretSharing.h"

#include "AttributionMetrics.h"
#include "fbpcs/emp_games/attribution/AttributionOptions.h"

namespace measurement::private_attribution {

// POTENTIAL OPTIMIZATION: Don't use such a large/small padding values, it will
//  prevent reducing the # of bits.
static const Touchpoint TOUCHPOINT_PADDING_VALUE{
    /* id */ INVALID_TP_ID,
    /* isClick */ false,
    /* ad_id */ -1,
    /* ts */ 0,
    /* campaignMetadata */ 0};

static const Conversion CONVERSION_PADDING_VALUE{
    /* ts */ -1,
    /* conv_value */ -1,
    /* metadata */ 0,
};

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
    for (std::vector<AggregationFormat>::size_type i = 0;
         i < aggregationFormats.size();
         i++) {
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

template <int MY_ROLE>
const std::vector<int64_t> shareValidAdIds(
    const std::vector<std::vector<Touchpoint>>& tpArrays) {
  int64_t numValidAdIds = 0;

  // Compute and then send over the number ad ids.
  std::vector<int64_t> adIds;
  if constexpr (MY_ROLE == PUBLISHER) {
    XLOG(DBG, "Computing valid ad ids for sending to partner");
    std::unordered_set<int64_t> adIdSet;
    for (auto& tpArray : tpArrays) {
      for (auto& tp : tpArray) {
        adIdSet.insert(tp.adId);
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

/**
 * Computes the ad attributions for the given id.
 */
const std::vector<PrivateAttribution> computeAttributionsForId(
    const int64_t id,
    const std::vector<PrivateTouchpoint>& touchpoints,
    const std::vector<PrivateConversion>& conversions,
    const AttributionRule& attributionRule) {
  std::vector<PrivateAttribution> attributions;
  for (const auto& conv : conversions) {
    OMNISCIENT_ONLY_XLOGF(
        DBG, "Computing attributions for conversion: {}", conv.reveal());

    // Start with an unattributed attribution
    PrivateAttribution attribution{
        /* uid */ id,
        /* hasAttributedTouchpoint */ emp::Bit{false},
        /* conv */ conv,
        /* tp */ PrivateTouchpoint{}};

    for (const auto& tp : touchpoints) {
      OMNISCIENT_ONLY_XLOGF(
          DBG, "Checking touchpoint: {}", tp.reveal(emp::PUBLIC));

      // Only use the new touchpoint if it's valid (not padding), attributable,
      // and it is preferred over the existing touchpoint
      auto isNewTouchpointAttributable =
          attributionRule.isAttributable(tp, conv);
      auto isNewTouchpointValid = tp.isValid;
      auto isExistingTouchpointInvalid = !attribution.hasAttributedTouchpoint;
      auto isNewTouchpointPreferred = isExistingTouchpointInvalid |
          attributionRule.isNewTouchpointPreferred(tp, attribution.tp);

      auto useNewTouchpoint = isNewTouchpointValid &
          isNewTouchpointAttributable & isNewTouchpointPreferred;

      OMNISCIENT_ONLY_XLOGF(
          DBG,
          "isNewTouchpointAttributable={}, isNewTouchpointValid={}, isExistingTouchpointInvalid={}, isNewTouchpointPreferred={}, useNewTouchpoint={}",
          isNewTouchpointAttributable.reveal<bool>(),
          isNewTouchpointValid.reveal<bool>(),
          isExistingTouchpointInvalid.reveal<bool>(),
          isNewTouchpointPreferred.reveal<bool>(),
          useNewTouchpoint.reveal<bool>());

      attribution = PrivateAttribution{
          /* uid */ id,
          /* hasAttributedTouchpoint */ attribution.hasAttributedTouchpoint |
              useNewTouchpoint,
          /* conv */ conv,
          /* tp */ attribution.tp.select(useNewTouchpoint, tp)};
    }

    attributions.push_back(attribution);
  }

  return attributions;
}

template <int MY_ROLE>
AttributionOutputMetrics computeAttributions(
    const AttributionInputMetrics& inputData,
    fbpcf::Visibility outputVisibility) {
  // TODO: verify that the ids are the same on both ends
  auto ids = inputData.getIds();
  uint32_t numIds = ids.size();
  XLOGF(INFO, "Have {} ids", numIds);

  // Send over all of the data needed for this computation
  XLOG(INFO, "Sharing attribution rules...");
  const auto attributionRules =
      shareAttributionRules<MY_ROLE>(inputData.getAttributionRules());
  XLOG(INFO, "Sharing aggregation formats...");
  const auto aggregationFormats =
      shareAggregationFormats<MY_ROLE>(inputData.getAggregationFormats());
  XLOG(INFO, "Sharing ad ids...");
  const auto adIds = shareValidAdIds<MY_ROLE>(inputData.getTouchpointArrays());
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

    // Compute all attributions for all rule/format combinations.
    PrivateAttributionMetrics attributionMetrics{
        attributionRule,
        aggregationFormats,
        AggregationContext{adIds, inputData.getIds(), tpArrays},
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
      for (auto attribution : attributionsForId) {
        attributionMetrics.addAttribution(attribution);
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
} // namespace measurement::private_attribution
