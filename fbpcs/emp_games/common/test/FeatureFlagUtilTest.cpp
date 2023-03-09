/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "fbpcs/emp_games/common/FeatureFlagUtil.h"
#include <gtest/gtest.h>

namespace private_measurement {
class FeatureFlagUtilTest : public ::testing::TestWithParam<
                                std::tuple<std::string, std::string, bool>> {};

TEST_P(FeatureFlagUtilTest, Parameterized) {
  // setup
  auto [flags, flag, expectedResult] = GetParam();
  // act
  bool actualResult = private_measurement::isFeatureFlagEnabled(flags, flag);
  // assert
  EXPECT_EQ(expectedResult, actualResult);
}

INSTANTIATE_TEST_SUITE_P(
    isFeatureFlagEnabled,
    FeatureFlagUtilTest,
    testing::ValuesIn(std::vector<std::tuple<std::string, std::string, bool>>{
        {"pcs_dummy_feature", "pcs_dummy_feature", true},
        {"pcs_dummy_feature", "pcs_fake_feature", false},
        {"pcs_dummy_feature,pcs_fake_feature", "pcs_fake_feature", true},
        {"pcs_dummy_feature,pcs_fake_feature", "pcs_feature_not_found", false},
        {"", "pcs_dummy_feature", false},
        {",pcs_dummy_feature,", "", false},
    }));
} // namespace private_measurement
