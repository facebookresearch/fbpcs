/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <gtest/gtest.h>

#include <fbpcf/mpc/EmpTestUtil.h>

#include "../AttributionRule.h"

namespace measurement::private_attribution {

PrivateTouchpoint createTouchpoint(bool isClick, int64_t ts) {
  return PrivateTouchpoint{
      emp::Bit{/*isValid*/ true},
      emp::Bit{isClick},
      emp::Integer{INT_SIZE, /*adID*/ 100},
      emp::Integer{TS_SIZE, ts},
      emp::Integer{INT_SIZE, /*id*/ 101},
      emp::Integer{INT_SIZE, /*campaignMetadata*/ 102}};
}

PrivateConversion createConversion(int64_t ts) {
  return PrivateConversion{
      emp::Integer{TS_SIZE, ts},
      emp::Integer{INT_SIZE, /*conv_value*/ 1000},
      emp::Integer{INT_SIZE, /*conv_metadata*/ 1001}};
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

    auto oldClickTp = createTouchpoint(/*isClick*/ true, 100);
    auto newClickTp = createTouchpoint(/*isClick*/ true, 200);

    auto oldImpTp = createTouchpoint(/*isClick*/ false, 100);
    auto newImpTp = createTouchpoint(/*isClick*/ false, 200);

    // Prefer the newer touchpoint if both are clicks
    EXPECT_TRUE(attributionRule.isNewTouchpointPreferred(newClickTp, oldClickTp)
                    .reveal<bool>());
    EXPECT_TRUE(attributionRule.isNewTouchpointPreferred(oldClickTp, oldClickTp)
                    .reveal<bool>());
    EXPECT_FALSE(
        attributionRule.isNewTouchpointPreferred(oldClickTp, newClickTp)
            .reveal<bool>());

    // Prefer the newer touchpoint if both are impressions
    EXPECT_TRUE(attributionRule.isNewTouchpointPreferred(newImpTp, oldImpTp)
                    .reveal<bool>());
    EXPECT_TRUE(attributionRule.isNewTouchpointPreferred(oldImpTp, oldImpTp)
                    .reveal<bool>());
    EXPECT_FALSE(attributionRule.isNewTouchpointPreferred(oldImpTp, newImpTp)
                     .reveal<bool>());

    if (!isClickOnlyAttributionRule) {
      // Prefer clicks over impressions
      EXPECT_TRUE(attributionRule.isNewTouchpointPreferred(oldClickTp, newImpTp)
                      .reveal<bool>());
      EXPECT_FALSE(
          attributionRule.isNewTouchpointPreferred(newImpTp, oldClickTp)
              .reveal<bool>());
    }
  });
}

INSTANTIATE_TEST_SUITE_P(
    AttributionRules,
    AttributionRuleTest,
    testing::Values(
        std::make_pair(1, 0),
        std::make_pair(28, 0),
        std::make_pair(1, 1),
        std::make_pair(28, 1)));

} // namespace measurement::private_attribution
