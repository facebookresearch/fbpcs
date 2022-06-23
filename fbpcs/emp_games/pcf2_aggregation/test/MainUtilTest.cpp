/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <gtest/gtest.h>

#include <string>
#include "folly/logging/xlog.h"

#include "fbpcs/emp_games/pcf2_aggregation/MainUtil.h"

namespace pcf2_aggregation {

TEST(MainUtilTest, AggregationMainUtilSingleFileWithPostFixTest) {
  std::string inputBasePath = "testPath";
  auto fileStartIndex = 0;
  auto inputFilePaths = pcf2_aggregation::getIOInputFilenames(
      1, inputBasePath, fileStartIndex, true);

  EXPECT_EQ(1, inputFilePaths.size());
  EXPECT_EQ(
      folly::sformat("{}_{}", inputBasePath, fileStartIndex),
      inputFilePaths.front());
}

TEST(MainUtilTest, AggregationMainUtilSingleFileWithoutPostFixTest) {
  auto inputBasePath = "testPath";
  auto fileStartIndex = 0;
  auto inputFilePaths = pcf2_aggregation::getIOInputFilenames(
      1, inputBasePath, fileStartIndex, false);

  EXPECT_EQ(1, inputFilePaths.size());
  EXPECT_EQ(inputBasePath, inputFilePaths.front());
}

TEST(MainUtilTest, AggregationMainUtilMultipleFilesTest) {
  auto inputBasePath = "testPath";
  auto fileStartIndex = 0;
  auto inputFilePaths = pcf2_aggregation::getIOInputFilenames(
      3, inputBasePath, fileStartIndex, true);

  EXPECT_EQ(3, inputFilePaths.size());
  EXPECT_EQ(
      folly::sformat("{}_{}", inputBasePath, fileStartIndex),
      inputFilePaths.front());
  EXPECT_EQ(
      folly::sformat("{}_{}", inputBasePath, fileStartIndex + 1),
      inputFilePaths[1]);
  EXPECT_EQ(
      folly::sformat("{}_{}", inputBasePath, fileStartIndex + 2),
      inputFilePaths[2]);
}

} // namespace pcf2_aggregation
