/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "../AddPaddingToCols.h"

#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <fstream>

#include <gflags/gflags.h>
#include <gtest/gtest.h>

using namespace ::pid::combiner;

class AddPaddingToColsTest : public testing::Test {
 public:
  void vectorStringToStream(
      std::vector<std::string>& input,
      std::stringstream& out) {
    for (auto const& row : input) {
      out << row << '\n';
    }
  }

  void validateOutputFile(std::vector<std::string>& expectedOutput) {
    // Validate the output with what is expected
    uint64_t lineIndex = 0;
    std::string outputString;
    while (getline(outputStream_, outputString)) {
      EXPECT_EQ(outputString, expectedOutput.at(lineIndex));
      ++lineIndex;
    }

    // Should not be any extra entries any side
    EXPECT_EQ(lineIndex, expectedOutput.size());
  }

  void runTest(
      std::vector<std::string>& dataContent,
      std::vector<std::string> cols,
      std::vector<int> pad_size_per_col,
      bool enforce_max,
      std::vector<std::string>& expectedOutput) {
    vectorStringToStream(dataContent, dataStream_);
    addPaddingToCols(
        dataStream_, cols, pad_size_per_col, enforce_max, outputStream_);
    validateOutputFile(expectedOutput);
  }

 protected:
  std::stringstream dataStream_;
  std::stringstream outputStream_;
};

// test basic padding
TEST_F(AddPaddingToColsTest, TestPaddingBasic) {
  std::vector<std::string> dataInput = {
      "id_,event_timestamp,value",
      "id_1,[125,126,390],[a,b,c]",
      "id_2,[200],[c]",
      "id_3,[375],[d]",
      "id_4,[400],[d]"};
  std::vector<std::string> expectedOutput = {
      "id_,event_timestamp,value",
      "id_1,[125,126,390],[0,a,b,c]",
      "id_2,[0,200],[0,0,0,c]",
      "id_3,[0,375],[0,0,0,d]",
      "id_4,[0,400],[0,0,0,d]"};
  runTest(
      dataInput, {"event_timestamp", "value"}, {2, 4}, false, expectedOutput);
}

// test max enforcement
TEST_F(AddPaddingToColsTest, TestMaxEnforcement) {
  std::vector<std::string> dataInput = {
      "id_,event_timestamp,value",
      "id_1,[125,126,390],[a,b,c]",
      "id_2,[200],[c]",
      "id_3,[375],[d]",
      "id_4,[400],[d]"};
  std::vector<std::string> expectedOutput = {
      "id_,event_timestamp,value",
      "id_1,[125,126],[0,a,b,c]",
      "id_2,[0,200],[0,0,0,c]",
      "id_3,[0,375],[0,0,0,d]",
      "id_4,[0,400],[0,0,0,d]"};
  runTest(
      dataInput, {"event_timestamp", "value"}, {2, 4}, true, expectedOutput);
}
