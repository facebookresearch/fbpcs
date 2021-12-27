/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "../SortIntegralValues.h"

#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <fstream>

#include <gflags/gflags.h>
#include <gtest/gtest.h>

using namespace ::pid::combiner;

class SortIntegralValuesTest : public testing::Test {
 public:
  void vectorStringToStream(
      const std::vector<std::string>& input,
      std::stringstream& out) {
    for (auto const& row : input) {
      out << row << '\n';
    }
  }

  void validateOutputFile(const std::vector<std::string>& expectedOutput) {
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
      const std::vector<std::string>& dataContent,
      const std::string& sortBy,
      const std::vector<std::string>& listColumns,
      std::vector<std::string>& expectedOutput) {
    vectorStringToStream(dataContent, inputStream_);
    sortIntegralValues(inputStream_, outputStream_, sortBy, listColumns);
    validateOutputFile(expectedOutput);
  }

 protected:
  std::stringstream inputStream_;
  std::stringstream outputStream_;
};

// test basic sorting
TEST_F(SortIntegralValuesTest, TestSortingBasic) {
  std::vector<std::string> dataInput = {
      "id_,event_timestamps,values",
      "id_1,[125,126,390],[a,b,c]",
      "id_2,[390,126,125],[a,b,c]",
      "id_3,[125,390,126],[a,b,c]",
  };
  std::vector<std::string> expectedOutput = {
      "id_,event_timestamps,values",
      "id_1,[125,126,390],[a,b,c]",
      "id_2,[125,126,390],[c,b,a]",
      "id_3,[125,126,390],[a,c,b]",
  };
  runTest(
      dataInput,
      "event_timestamps",
      {"event_timestamps", "values"},
      expectedOutput);
}
