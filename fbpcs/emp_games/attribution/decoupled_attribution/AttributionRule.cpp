/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "fbpcs/emp_games/attribution/decoupled_attribution/AttributionRule.h"

namespace aggregation::private_attribution {

const AttributionRule LAST_CLICK_1D{
    /* id */ 1,
    /* name */ "last_click_1d",
    /* window_in_sec */ 86400,
    /* isAttributable */
    [](const PrivateTouchpoint& tp,
       const PrivateConversion& conv) -> const emp::Bit {
      auto secondsInOneDay = 86400;
      return tp.isClick &
          ((conv.ts > tp.ts) & (conv.ts - tp.ts < secondsInOneDay));
    },
    /* isNewTouchpointPreferred: Select the most recent touchpoint */
    [](const PrivateTouchpoint& newTp, const PrivateTouchpoint& oldTp)
        -> emp::Bit { return newTp.ts >= oldTp.ts; }};

/**
 * Attribute if the conversion took place within 28 days of the touchpoint
 */
const AttributionRule LAST_CLICK_28D{
    /* id */ 2,
    /* name */ "last_click_28d",
    /* window_in_sec */ 2419200,
    /* isAttributable */
    [](const PrivateTouchpoint& tp,
       const PrivateConversion& conv) -> const emp::Bit {
      auto secondsInTwentyEightDays = 2419200;
      return tp.isClick &
          ((conv.ts > tp.ts) & (conv.ts - tp.ts < secondsInTwentyEightDays));
    },
    /* isNewTouchpointPreferred: Select the most recent touchpoint */
    [](const PrivateTouchpoint& newTp, const PrivateTouchpoint& oldTp)
        -> emp::Bit { return newTp.ts >= oldTp.ts; }};

/**
 * The last touch attribution model gives 100% of the credit for a conversion to
 * the last click that happened in a conversion path. If there was no
 * click, then it will credit the last impression.
 */
const AttributionRule LAST_TOUCH_CT1D_IMP1D{
    /* id */ 3,
    /* name */ "last_touch_1d",
    /* window_in_sec */ 86400,
    /* isAttributable: if click within 1d, if touch within 1d */
    [](const PrivateTouchpoint& tp,
       const PrivateConversion& conv) -> const emp::Bit {
      auto secondsInOneDay = 86400;

      auto validConv = conv.ts > tp.ts;
      auto convDelta = conv.ts - tp.ts;
      auto within1d = validConv & (convDelta < secondsInOneDay);

      return within1d;
    },
    /* isNewTouchpointPreferred: if both clicks, select the most recent. if
       the new touchpoint is a click, but the old is a view, prefer the click.
     */
    [](const PrivateTouchpoint& newTp,
       const PrivateTouchpoint& oldTp) -> emp::Bit {
      auto isSameKindOfTouchpoint = newTp.isClick == oldTp.isClick;
      auto isNewTpMoreRecent = newTp.ts >= oldTp.ts;

      // emp::If(condition, true_case, false_case)
      return emp::If(isSameKindOfTouchpoint, isNewTpMoreRecent, newTp.isClick);
    }};

const AttributionRule LAST_TOUCH_CT28D_IMP1D{
    /* id */ 4,
    /* name */ "last_touch_28d",
    /* window_in_sec */ 2419200,
    /* isAttributable: if click within 28d, if touch within 1d */
    [](const PrivateTouchpoint& tp,
       const PrivateConversion& conv) -> const emp::Bit {
      auto secondsInOneDay = 86400;
      auto secondsInTwentyEightDays = 2419200;

      auto validConv = conv.ts > tp.ts;
      auto convDelta = conv.ts - tp.ts;
      auto within1d = validConv & (convDelta < secondsInOneDay);
      auto within28d = validConv & (convDelta < secondsInTwentyEightDays);

      // emp::If(condition, true_case, false_case)
      return emp::If(tp.isClick, within28d, within1d);
    },
    /* isNewTouchpointPreferred: if both clicks, select the most recent. if
       the new touchpoint is a click, but the old is a view, prefer the click.
     */
    [](const PrivateTouchpoint& newTp,
       const PrivateTouchpoint& oldTp) -> emp::Bit {
      auto isSameKindOfTouchpoint = newTp.isClick == oldTp.isClick;
      auto isNewTpMoreRecent = newTp.ts >= oldTp.ts;

      // emp::If(condition, true_case, false_case)
      return emp::If(isSameKindOfTouchpoint, isNewTpMoreRecent, newTp.isClick);
    }};

const AttributionRule SUPPORTED_ATTRIBUTION_RULES[]{
    LAST_CLICK_1D,
    LAST_CLICK_28D,
    LAST_TOUCH_CT1D_IMP1D,
    LAST_TOUCH_CT28D_IMP1D};

const AttributionRule AttributionRule::fromNameOrThrow(
    const std::string& name) {
  for (auto rule : SUPPORTED_ATTRIBUTION_RULES) {
    if (rule.name == name) {
      return rule;
    }
  }

  throw std::runtime_error("Unknown attribution rule name: " + name);
}

const AttributionRule AttributionRule::fromIdOrThrow(int64_t id) {
  for (auto rule : SUPPORTED_ATTRIBUTION_RULES) {
    if (rule.id == id) {
      return rule;
    }
  }

  throw std::runtime_error(fmt::format("Unknown attribution id: {}", id));
}

}; // namespace aggregation::private_attribution
