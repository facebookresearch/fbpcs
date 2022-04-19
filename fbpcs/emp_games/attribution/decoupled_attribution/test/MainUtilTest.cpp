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

#include "fbpcs/emp_games/attribution/decoupled_attribution/MainUtil.h"

namespace aggregation::private_attribution {

TEST(AttributionMainUtilSingleFileWithPostFixTest, TestConstructor) {
  fbpcf::mpc::wrapTest<std::function<void()>>([]() {
    string inputBasePath = "testInputPath";
    string outputBasePath = "testOutputPath";
    auto fileStartIndex = 0;

    auto [inputFilePaths, outputFilePaths] =
        aggregation::private_attribution::getIOFilenames(
            1, inputBasePath, outputBasePath, fileStartIndex, true);

    EXPECT_EQ(1, inputFilePaths.size());
    EXPECT_EQ(1, outputFilePaths.size());

    EXPECT_EQ(
        folly::sformat("{}_{}", inputBasePath, fileStartIndex),
        inputFilePaths.front());

    EXPECT_EQ(
        folly::sformat("{}_{}", outputBasePath, fileStartIndex),
        outputFilePaths.front());
  });
}

TEST(AttributionMainUtilSingleFileWithoutPostFixTest, TestConstructor) {
  fbpcf::mpc::wrapTest<std::function<void()>>([]() {
    string inputBasePath = "testInputPath";
    string outputBasePath = "testOutputPath";
    auto fileStartIndex = 0;
    auto [inputFilePaths, outputFilePaths] =
        aggregation::private_attribution::getIOFilenames(
            1, inputBasePath, outputBasePath, fileStartIndex, false);

    EXPECT_EQ(1, inputFilePaths.size());
    EXPECT_EQ(1, outputFilePaths.size());

    EXPECT_EQ(inputBasePath, inputFilePaths.front());
    EXPECT_EQ(outputBasePath, outputFilePaths.front());
  });
}

TEST(AttributionMainUtilMultipleFilesTest, TestConstructor) {
  fbpcf::mpc::wrapTest<std::function<void()>>([]() {
    string inputBasePath = "testInputPath";
    string outputBasePath = "testOutputPath";
    auto fileStartIndex = 0;
    auto [inputFilePaths, outputFilePaths] =
        aggregation::private_attribution::getIOFilenames(
            3, inputBasePath, outputBasePath, fileStartIndex, true);

    EXPECT_EQ(3, inputFilePaths.size());
    EXPECT_EQ(3, outputFilePaths.size());
    EXPECT_EQ(
        folly::sformat("{}_{}", inputBasePath, fileStartIndex),
        inputFilePaths.front());
    EXPECT_EQ(
        folly::sformat("{}_{}", outputBasePath, fileStartIndex),
        outputFilePaths.front());
  });
}

} // namespace aggregation::private_attribution
