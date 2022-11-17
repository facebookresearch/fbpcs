/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "fbpcs/data_processing/common/Logging.h"

#include <gtest/gtest.h>
#include <cstdint>
#include <string>

using namespace ::testing;

namespace private_lift::logging {

struct TestData {
  std::uint64_t inputNum;
  std::string expected;
};

class LoggingNumberFormatTests : public testing::TestWithParam<TestData> {};

TEST_P(LoggingNumberFormatTests, FormatTest) {
  auto& inputNum = GetParam().inputNum;
  auto& expected = GetParam().expected;

  auto result = formatNumber(inputNum);

  EXPECT_EQ(result, expected);
}

INSTANTIATE_TEST_SUITE_P(
    BasicFormatTest,
    LoggingNumberFormatTests,
    testing::Values(
        TestData{1'000'000'000, "1.00B"},
        TestData{1'000'000, "1.00M"},
        TestData{1'000, "1.00K"}));

INSTANTIATE_TEST_SUITE_P(
    FormatTest_PrecisionToTwoDigits,
    LoggingNumberFormatTests,
    testing::Values(
        TestData{5'784'123'345, "5.78B"},
        TestData{6'123'799, "6.12M"},
        TestData{9'743, "9.74K"}));

INSTANTIATE_TEST_SUITE_P(
    FormatTest_PrecisionToTwoDigits_RoundedUp,
    LoggingNumberFormatTests,
    testing::Values(
        TestData{5'786'123'345, "5.79B"},
        TestData{6'128'799, "6.13M"},
        TestData{9'748, "9.75K"}));

INSTANTIATE_TEST_SUITE_P(
    FormatTest_LessThan1k_NoFormatting,
    LoggingNumberFormatTests,
    testing::Values(TestData{345, "345"}, TestData{999, "999"}));

} // namespace private_lift::logging
