/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <gtest/gtest.h>

#include "../Csv.h"

namespace private_measurement {
class CsvTest : public ::testing::Test {
 private:
 protected:
  void SetUp() override {}
};

TEST_F(CsvTest, TestSplitByCommaNotSupportInnerBrackets) {
  std::string inputStr =
      " 43feaeeecd7b2fe2ae2e26d917b6477d , 1 , 0 , 1600000192   ";
  std::vector<std::string> expOutput = {
      "43feaeeecd7b2fe2ae2e26d917b6477d", "1", "0", "1600000192"};
  auto output = csv::splitByComma(inputStr, false);
  EXPECT_EQ(expOutput, output);
}

TEST_F(CsvTest, TestSplitByCommaSupportInnerBrackets) {
  std::string inputStr =
      "  c4ca4238a0b923820dcc509a6f75849b,  [0, 0, 1600000330, 1600000594],  [0, 0, 71, 71] ";
  std::vector<std::string> expOutput = {
      "c4ca4238a0b923820dcc509a6f75849b",
      "[0,0,1600000330,1600000594]",
      "[0,0,71,71]"};
  auto output = csv::splitByComma(inputStr, true);
  EXPECT_EQ(expOutput, output);
}

} // namespace private_measurement
