/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "../IdSwap.h"

#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <fstream>

#include <gflags/gflags.h>
#include <gtest/gtest.h>

#include <folly/logging/xlog.h>

class IdSwapTest : public testing::Test {
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
    pid::combiner::idSwap(dataStream_, spineStream_, outputStream_);
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
TEST_F(IdSwapTest, ValidSpinePublisher) {
  std::vector<std::string> dataInput = {
      "id_,opportunity_timestamp,test_flag",
      "123,100,1",
      "456,150,0",
      "789,200,0"};
  std::vector<std::string> spineInput = {
      "AAAA,123", "BBBB,", "CCCC,456", "DDDD,789", "EEEE,", "FFFF,"};
  std::vector<std::string> expectedOutput = {
      "id_,opportunity_timestamp,test_flag",
      "AAAA,100,1",
      "CCCC,150,0",
      "DDDD,200,0",
  };
  runTest(dataInput, spineInput, expectedOutput);
}

// The only reason that this is a separate test is because we insert a column
// at the end and then check where the opportunity_timestamp column exists.
// This led to a bug since we threw a std::out_of_range in a real test.
TEST_F(IdSwapTest, ValidSpinePublisherTimestampLastColumn) {
  std::vector<std::string> dataInput = {
      "id_,test_flag,opportunity_timestamp",
      "123,1,100",
      "456,0,150",
      "789,0,200"};
  std::vector<std::string> spineInput = {
      "AAAA,123", "BBBB,", "CCCC,456", "DDDD,789", "EEEE,", "FFFF,"};
  std::vector<std::string> expectedOutput = {
      "id_,test_flag,opportunity_timestamp",
      "AAAA,1,100",
      "CCCC,0,150",
      "DDDD,0,200"};
  runTest(dataInput, spineInput, expectedOutput);
}

// Valid spine with some amount of overlap for partner
// No opp_flag flag needed at the output level
TEST_F(IdSwapTest, ValidSpinePartner) {
  std::vector<std::string> dataInput = {
      "id_,event_timestamp,value",
      "123,125,100",
      "111,200,200",
      "222,375,300",
      "333,400,400"};
  std::vector<std::string> spineInput = {
      "AAAA,123", "BBBB,111", "CCCC,", "DDDD,", "EEEE,222", "FFFF,333"};
  std::vector<std::string> expectedOutput = {
      "id_,event_timestamp,value",
      "AAAA,125,100",
      "BBBB,200,200",
      "EEEE,375,300",
      "FFFF,400,400"};
  runTest(dataInput, spineInput, expectedOutput);
}

// Test with IdColumnIndex not at 0
TEST_F(IdSwapTest, IdColumnIndexNotZero) {
  std::vector<std::string> dataInput = {
      "event_timestamp,id_,value",
      "125,123,100",
      "200,111,200",
      "375,222,300",
      "400,333,400"};
  std::vector<std::string> spineInput = {
      "AAAA,123", "BBBB,111", "CCCC,", "DDDD,", "EEEE,222", "FFFF,333"};
  std::vector<std::string> expectedOutput = {
      "event_timestamp,id_,value",
      "125,AAAA,100",
      "200,BBBB,200",
      "375,EEEE,300",
      "400,FFFF,400"};
  runTest(dataInput, spineInput, expectedOutput);
}

// Test with IdColumnIndex at last col
TEST_F(IdSwapTest, IdColumnIndexLastCol) {
  std::vector<std::string> dataInput = {
      "event_timestamp,value,id_",
      "125,100,123",
      "200,200,111",
      "375,300,222",
      "400,400,333"};
  std::vector<std::string> spineInput = {
      "AAAA,123", "BBBB,111", "CCCC,", "DDDD,", "EEEE,222", "FFFF,333"};
  std::vector<std::string> expectedOutput = {
      "event_timestamp,value,id_",
      "125,100,AAAA",
      "200,200,BBBB",
      "375,300,EEEE",
      "400,400,FFFF"};
  runTest(dataInput, spineInput, expectedOutput);
}

// Id missing in spine
// We'd expect an error to be thrown here
// Some mismatch between pid service output and dataFile
TEST_F(IdSwapTest, MissingPrivateIdsSpine) {
  std::vector<std::string> dataInput = {
      "id_,event_timestamp,value",
      "123,125,100",
      "111,200,200",
      "222,375,300",
      "333,400,400"};
  std::vector<std::string> spineInput = {
      "BBBB,111", "CCCC,", "DDDD,", "EEEE,222", "FFFF,333"};
  std::vector<std::string> expectedOutput = {
      "id_,event_timestamp,value",
      "BBBB,200,200",
      "EEEE,375,300",
      "FFFF,400,400"};

  vectorStringToStream(dataInput, dataStream_);
  vectorStringToStream(spineInput, spineStream_);

  ASSERT_DEATH(
      pid::combiner::idSwap(dataStream_, spineStream_, outputStream_),
      "ID is missing in the spineID file");
}

// Spine id contains an id_ that doesn't exist in data
// IdSwap doesnt do anything since insert handles this
TEST_F(IdSwapTest, MissingPrivateIdsInData) {
  std::vector<std::string> dataInput = {
      "id_,event_timestamp,value", "111,200,200", "222,375,300", "333,400,400"};
  std::vector<std::string> spineInput = {
      "BBBB,111", "CCCC,", "DDDD,444", "EEEE,222", "FFFF,333"};
  std::vector<std::string> expectedOutput = {
      "id_,event_timestamp,value",
      "BBBB,200,200",
      "EEEE,375,300",
      "FFFF,400,400"};

  runTest(dataInput, spineInput, expectedOutput);
}
