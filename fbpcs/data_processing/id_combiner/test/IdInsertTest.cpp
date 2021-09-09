/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "../IdInsert.h"

#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <fstream>

#include <gflags/gflags.h>
#include <gtest/gtest.h>

#include <folly/logging/xlog.h>

class IdInsertTest : public testing::Test {
 public:
  void vectorStringToStream(
      std::vector<std::string>& input,
      std::stringstream& out) {
    for (auto const& row : input) {
      out << row << '\n';
    }
  }

  void validateOutputContent(std::vector<std::string>& expectedOutput) {
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
      std::vector<std::string>& dataInput,
      std::vector<std::string>& spineInput,
      std::vector<std::string>& expectedOutput) {
    // Execute the union pid combiner with the pre-created files

    vectorStringToStream(dataInput, dataStream_);
    vectorStringToStream(spineInput, spineStream_);
    pid::combiner::idInsert(dataStream_, spineStream_, outputStream_);
    validateOutputContent(expectedOutput);
  }

 protected:
  std::stringstream dataStream_;
  std::stringstream spineStream_;
  std::stringstream outputStream_;
};

// Valid spine with some amount of overlap for publisher
// As this is publisher data the opp_flag flag needs to be created in the
// program itself
TEST_F(IdInsertTest, ValidSpinePublisher) {
  std::vector<std::string> dataInput = {
      "id_,opportunity_timestamp,test_flag",
      "AAAA,100,1",
      "CCCC,150,0",
      "DDDD,200,0"};
  std::vector<std::string> spineInput = {
      "AAAA,123", "BBBB,", "CCCC,456", "DDDD,789", "EEEE,", "FFFF,"};
  std::vector<std::string> expectedOutput = {
      "id_,opportunity_timestamp,test_flag",
      "AAAA,100,1",
      "BBBB,0,0",
      "CCCC,150,0",
      "DDDD,200,0",
      "EEEE,0,0",
      "FFFF,0,0"};
  runTest(dataInput, spineInput, expectedOutput);
}

// Test with IdColumnIndex not at 0
TEST_F(IdInsertTest, IdColumnIndexNotZero) {
  std::vector<std::string> dataInput = {
      "event_timestamp,id_,value",
      "125,AAAA,100",
      "200,BBBB,200",
      "375,EEEE,300",
      "400,FFFF,400"};
  std::vector<std::string> spineInput = {
      "AAAA,123", "BBBB,111", "CCCC,", "DDDD,", "EEEE,222", "FFFF,333"};
  std::vector<std::string> expectedOutput = {
      "event_timestamp,id_,value",
      "125,AAAA,100",
      "200,BBBB,200",
      "0,CCCC,0",
      "0,DDDD,0",
      "375,EEEE,300",
      "400,FFFF,400"};
  runTest(dataInput, spineInput, expectedOutput);
}

// Test with IdColumnIndex at last col
TEST_F(IdInsertTest, IdColumnIndexLastCol) {
  std::vector<std::string> dataInput = {
      "event_timestamp,value,id_",
      "125,100,AAAA",
      "200,200,BBBB",
      "375,300,EEEE",
      "400,400,FFFF"};
  std::vector<std::string> spineInput = {
      "AAAA,123", "BBBB,111", "CCCC,", "DDDD,", "EEEE,222", "FFFF,333"};
  std::vector<std::string> expectedOutput = {
      "event_timestamp,value,id_",
      "125,100,AAAA",
      "200,200,BBBB",
      "0,0,CCCC",
      "0,0,DDDD",
      "375,300,EEEE",
      "400,400,FFFF"};
  runTest(dataInput, spineInput, expectedOutput);
}

// Rows with duplicate private_ids
// We would expect the data to flow down as the same
TEST_F(IdInsertTest, DuplicatePrivateIdsData) {
  std::vector<std::string> dataInput = {
      "id_,event_timestamp,value",
      "AAAA,125,100",
      "AAAA, 200, 240",
      "BBBB,200,200",
      "EEEE,375,300",
      "EEEE,700, 900",
      "FFFF,400,400"};
  std::vector<std::string> spineInput = {
      "AAAA,1234", "BBBB,111", "CCCC,", "DDDD,", "EEEE,222", "FFFF,333"};
  std::vector<std::string> expectedOutput = {
      "id_,event_timestamp,value",
      "AAAA,125,100",
      "AAAA, 200, 240",
      "BBBB,200,200",
      "CCCC,0,0",
      "DDDD,0,0",
      "EEEE,375,300",
      "EEEE,700, 900",
      "FFFF,400,400"};
}
