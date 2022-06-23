/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <gtest/gtest.h>

#include <string>
#include "folly/logging/xlog.h"

#include "fbpcs/emp_games/pcf2_attribution/MainUtil.h"

namespace pcf2_attribution {

TEST(MainUtilTest, AttributionMainUtilSingleFileWithPostFixTest) {
  std::string inputBasePath = "testInputPath";
  std::string outputBasePath = "testOutputPath";
  auto fileStartIndex = 0;

  auto [inputFilePaths, outputFilePaths] = pcf2_attribution::getIOFilenames(
      1, inputBasePath, outputBasePath, fileStartIndex, true);

  EXPECT_EQ(1, inputFilePaths.size());
  EXPECT_EQ(1, outputFilePaths.size());

  EXPECT_EQ(
      folly::sformat("{}_{}", inputBasePath, fileStartIndex),
      inputFilePaths.front());

  EXPECT_EQ(
      folly::sformat("{}_{}", outputBasePath, fileStartIndex),
      outputFilePaths.front());
}

TEST(MainUtilTest, AttributionMainUtilSingleFileWithoutPostFixTest) {
  std::string inputBasePath = "testInputPath";
  std::string outputBasePath = "testOutputPath";
  auto fileStartIndex = 0;
  auto [inputFilePaths, outputFilePaths] = pcf2_attribution::getIOFilenames(
      1, inputBasePath, outputBasePath, fileStartIndex, false);

  EXPECT_EQ(1, inputFilePaths.size());
  EXPECT_EQ(1, outputFilePaths.size());

  EXPECT_EQ(inputBasePath, inputFilePaths.front());
  EXPECT_EQ(outputBasePath, outputFilePaths.front());
}

TEST(MainUtilTest, AttributionMainUtilMultipleFilesTest) {
  std::string inputBasePath = "testInputPath";
  std::string outputBasePath = "testOutputPath";
  auto fileStartIndex = 0;
  auto [inputFilePaths, outputFilePaths] = pcf2_attribution::getIOFilenames(
      3, inputBasePath, outputBasePath, fileStartIndex, true);

  EXPECT_EQ(3, inputFilePaths.size());
  EXPECT_EQ(3, outputFilePaths.size());
  EXPECT_EQ(
      folly::sformat("{}_{}", inputBasePath, fileStartIndex),
      inputFilePaths.front());
  EXPECT_EQ(
      folly::sformat("{}_{}", outputBasePath, fileStartIndex),
      outputFilePaths.front());
  EXPECT_EQ(
      folly::sformat("{}_{}", inputBasePath, fileStartIndex + 1),
      inputFilePaths[1]);
  EXPECT_EQ(
      folly::sformat("{}_{}", outputBasePath, fileStartIndex + 1),
      outputFilePaths[1]);
  EXPECT_EQ(
      folly::sformat("{}_{}", inputBasePath, fileStartIndex + 2),
      inputFilePaths[2]);
  EXPECT_EQ(
      folly::sformat("{}_{}", outputBasePath, fileStartIndex + 2),
      outputFilePaths[2]);
}

} // namespace pcf2_attribution
