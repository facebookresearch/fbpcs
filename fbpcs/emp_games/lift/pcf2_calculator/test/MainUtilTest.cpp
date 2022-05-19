/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <gtest/gtest.h>

#include "fbpcs/emp_games/lift/pcf2_calculator/MainUtil.h"

namespace private_lift {

TEST(MainUtilTest, TestInputBasePathEmptyInputDirectory) {
  auto filepaths =
      getIOFilepaths("inputBasePath", "outputBasePath", "", "", "", "", 3, 0);
  EXPECT_EQ(
      filepaths.first,
      (std::vector<std::string>{
          "inputBasePath_0", "inputBasePath_1", "inputBasePath_2"}));
  EXPECT_EQ(
      filepaths.second,
      (std::vector<std::string>{
          "outputBasePath_0", "outputBasePath_1", "outputBasePath_2"}));
}

TEST(MainUtilTest, TestInputBasePathNonEmptyInputDirectory) {
  auto filepaths = getIOFilepaths(
      "inputBasePath",
      "outputBasePath",
      "inputDirectory",
      "outputDirectory",
      "input.csv",
      "output.csv",
      3,
      0);
  EXPECT_EQ(
      filepaths.first,
      (std::vector<std::string>{
          "inputBasePath_0", "inputBasePath_1", "inputBasePath_2"}));
  EXPECT_EQ(
      filepaths.second,
      (std::vector<std::string>{
          "outputBasePath_0", "outputBasePath_1", "outputBasePath_2"}));
}

TEST(MainUtilTest, TestInputBasePathNumFilesZero) {
  auto filepaths = getIOFilepaths(
      "inputBasePath",
      "outputBasePath",
      "inputDirectory",
      "outputDirectory",
      "input.csv",
      "output.csv",
      0,
      1);
  EXPECT_EQ(filepaths.first.size(), 0);
  EXPECT_EQ(filepaths.second.size(), 0);
}

TEST(MainUtilTest, TestInputDirectory) {
  auto filepaths = getIOFilepaths(
      "",
      "",
      "inputDirectory",
      "outputDirectory",
      "input1.csv,input2.csv",
      "output1.csv,output2.csv",
      0,
      1);
  EXPECT_EQ(
      filepaths.first,
      (std::vector<std::string>{
          "inputDirectory/input1.csv", "inputDirectory/input2.csv"}));
  EXPECT_EQ(
      filepaths.second,
      (std::vector<std::string>{
          "outputDirectory/output1.csv", "outputDirectory/output2.csv"}));
}

} // namespace private_lift
