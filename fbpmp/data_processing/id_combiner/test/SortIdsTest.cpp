/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "../SortIds.h"

#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <fstream>

#include <gflags/gflags.h>
#include <gtest/gtest.h>

class SortIdsTest : public testing::Test {
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
      std::vector<std::string>& expectedOutput) {
    vectorStringToStream(dataContent, dataStream_);

    pid::combiner::sortIds(dataStream_, outputStream_);
    validateOutputFile(expectedOutput);
  }

 protected:
  std::stringstream dataStream_;
  std::stringstream outputStream_;
};

// testing group by first col over 1 other col
TEST_F(SortIdsTest, TestGroupingOverSomeCols) {
  std::vector<std::string> dataInput = {
      "id_,event_timestamp,value",
      "id_3,[375],d",
      "id_4,[400],d",
      "id_1,[125,126,390],a",
      "id_2,[200],c",
  };
  std::vector<std::string> expectedOutput = {
      "id_,event_timestamp,value",
      "id_1,[125,126,390],a",
      "id_2,[200],c",
      "id_3,[375],d",
      "id_4,[400],d"};
  runTest(dataInput, expectedOutput);
}

// testing group by second col over 1 other col
TEST_F(SortIdsTest, TestGroupingBySecondColOverSomeCols) {
  std::vector<std::string> dataInput = {
      "event_timestamp,id_,value",
      "[125,126,390],id_2,a",
      "[200],id_1,c",
      "[375],id_3,d",
      "[400],id_4,d"};
  std::vector<std::string> expectedOutput = {
      "event_timestamp,id_,value",
      "[200],id_1,c",
      "[125,126,390],id_2,a",
      "[375],id_3,d",
      "[400],id_4,d"};

  runTest(dataInput, expectedOutput);
}

// testing group by second col over 1 other col
TEST_F(SortIdsTest, TestGroupingTraversedOrder) {
  std::vector<std::string> dataInput = {
      "id_,event_timestamp,value",
      "BBB,[200],[200]",
      "AAA,[125,126,127,128,129],[102,103,104,105,106]",
      "DDD,[400],[400]",
      "CCC,[375],[300]",
  };
  std::vector<std::string> expectedOutput = {
      "id_,event_timestamp,value",
      "AAA,[125,126,127,128,129],[102,103,104,105,106]",
      "BBB,[200],[200]",
      "CCC,[375],[300]",
      "DDD,[400],[400]",
  };
  runTest(dataInput, expectedOutput);
}
