/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "../DataValidation.h"

#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <fstream>

#include <gflags/gflags.h>
#include <gtest/gtest.h>

using namespace ::pid::combiner;

class DataValidationTest : public testing::Test {
 public:
  void vectorStringToStream(
      std::vector<std::string>& input,
      std::stringstream& out) {
    for (auto const& row : input) {
      out << row << '\n';
    }
  }

  void runTest(std::vector<std::string>& dataContent) {
    vectorStringToStream(dataContent, dataStream_);
    validateCsvData(dataStream_);
  }

 protected:
  std::stringstream dataStream_;
};

TEST_F(DataValidationTest, TestValidData) {
  std::vector<std::string> dataInput = {
      "id_,event_timestamp,value",
      "123,125,100",
      "111,200,200",
      "222,375,300",
      "333,400,400"};

  runTest(dataInput);
}

TEST_F(DataValidationTest, TestInvalidData) {
  std::vector<std::string> dataInput = {
      "id_,event_timestamp,value",
      "abc,cdf,100",
      "111,200gh,200",
      "222,375,300",
      "333,400,400"};

  ASSERT_DEATH(
      runTest(dataInput),
      ".*in input file is not a number. Please validate your input.*");
}

TEST_F(DataValidationTest, TestRowMismatch) {
  std::vector<std::string> dataInput = {
      "id_,event_timestamp,value", "111,200,200", "222,375", "333,400,400"};

  ASSERT_DEATH(
      runTest(dataInput),
      "Row at index <2> and header sizes mismatch. Row size is 2 and header size is 3");
}
