/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <gtest/gtest.h>

#include <fbpcf/mpc/EmpTestUtil.h>

#include "fbpcs/emp_games/attribution/decoupled_attribution/AttributionRule.h"

namespace aggregation::private_attribution {

PrivateTouchpoint createTouchpoint(bool isClick, int64_t ts) {
  return PrivateTouchpoint{
      emp::Bit{isClick},
      emp::Integer{TS_SIZE, ts},
      emp::Integer{INT_SIZE, /*adID*/ 100}};
}

PrivateConversion createConversion(int64_t ts) {
  return PrivateConversion{emp::Integer{TS_SIZE, ts}};
}

class AttributionRuleTest
    : public testing::TestWithParam<std::pair<int64_t, int64_t>> {};

TEST_P(AttributionRuleTest, TestRule) {
  fbpcf::mpc::wrapTest<std::function<void()>>([]() {
    auto [clickWindowDurationInDays, impWindowDurationInDays] = GetParam();
    auto isClickOnlyAttributionRule = impWindowDurationInDays == 0;
    auto attributionRule = AttributionRule::fromNameOrThrow(
        (isClickOnlyAttributionRule ? "last_click_" : "last_touch_") +
        std::to_string(clickWindowDurationInDays) + "d");

    auto tpTime = 100;
    auto validClickConvTime = tpTime + clickWindowDurationInDays * 86400 - 1;

    // Valid click conversion
    auto tp = createTouchpoint(/*isClick*/ true, tpTime);
    auto conv = createConversion(validClickConvTime);

    EXPECT_TRUE(attributionRule.isAttributable(tp, conv).reveal<bool>());

    if (isClickOnlyAttributionRule) {
      // Not click conversion
      tp = createTouchpoint(/*isClick*/ false, tpTime);
      conv = createConversion(validClickConvTime);

      EXPECT_FALSE(attributionRule.isAttributable(tp, conv).reveal<bool>());
    } else {
      // Valid impression conversion
      tp = createTouchpoint(/*isClick*/ false, tpTime);
      conv = createConversion(tpTime + impWindowDurationInDays * 86400 - 1);

      EXPECT_TRUE(attributionRule.isAttributable(tp, conv).reveal<bool>());
    }

    // Conversion did not occur after touchpoint
    tp = createTouchpoint(/*isClick*/ true, tpTime);
    conv = createConversion(tpTime);

    EXPECT_FALSE(attributionRule.isAttributable(tp, conv).reveal<bool>());

    // Click conversion occurred after window ended
    tp = createTouchpoint(/*isClick*/ true, tpTime);
    conv = createConversion(tpTime + clickWindowDurationInDays * 86400);

    EXPECT_FALSE(attributionRule.isAttributable(tp, conv).reveal<bool>());

    // Impression conversion occurred after window ended
    tp = createTouchpoint(/*isClick*/ false, tpTime);
    conv = createConversion(tpTime + impWindowDurationInDays * 86400);

    EXPECT_FALSE(attributionRule.isAttributable(tp, conv).reveal<bool>());
  });
}

INSTANTIATE_TEST_SUITE_P(
    AttributionRules,
    AttributionRuleTest,
    testing::Values(std::make_pair(1, 0), std::make_pair(1, 1)));

} // namespace aggregation::private_attribution
