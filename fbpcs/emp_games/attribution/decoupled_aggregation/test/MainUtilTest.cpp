/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <emp-sh2pc/emp-sh2pc.h>
#include <gtest/gtest.h>

#include <fbpcf/mpc/EmpTestUtil.h>
#include <string>
#include "folly/logging/xlog.h"

#include "fbpcs/emp_games/attribution/decoupled_aggregation/MainUtil.h"

namespace aggregation::private_aggregation {

TEST(AggregationMainUtilSingleFileWithPostFixTest, TestConstructor) {
  fbpcf::mpc::wrapTest<std::function<void()>>([]() {
    string inputBasePath = "testPath";
    auto fileStartIndex = 0;
    auto inputFilePaths = aggregation::private_aggregation::getIOInputFilenames(
        1, inputBasePath, fileStartIndex, true);

    EXPECT_EQ(1, inputFilePaths.size());
    string expected_path = inputBasePath + "_";
    EXPECT_EQ(
        folly::sformat("{}_{}", inputBasePath, fileStartIndex),
        inputFilePaths.front());
  });
}

TEST(AggregationMainUtilSingleFileWithoutPostFixTest, TestConstructor) {
  fbpcf::mpc::wrapTest<std::function<void()>>([]() {
    auto inputBasePath = "testPath";
    auto fileStartIndex = 0;
    auto inputFilePaths = aggregation::private_aggregation::getIOInputFilenames(
        1, inputBasePath, fileStartIndex, false);

    EXPECT_EQ(1, inputFilePaths.size());
    EXPECT_EQ(inputBasePath, inputFilePaths.front());
  });
}

TEST(AggregationMainUtilMultipleFilesTest, TestConstructor) {
  fbpcf::mpc::wrapTest<std::function<void()>>([]() {
    auto inputBasePath = "testPath";
    auto fileStartIndex = 0;
    auto inputFilePaths = aggregation::private_aggregation::getIOInputFilenames(
        3, inputBasePath, fileStartIndex, true);

    EXPECT_EQ(3, inputFilePaths.size());
    EXPECT_EQ(
        folly::sformat("{}_{}", inputBasePath, fileStartIndex),
        inputFilePaths.front());
  });
}

} // namespace aggregation::private_aggregation
