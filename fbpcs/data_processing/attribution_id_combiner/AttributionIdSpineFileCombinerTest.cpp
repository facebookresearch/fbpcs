/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "AttributionIdSpineFileCombiner.h"

#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <fstream>

#include <gflags/gflags.h>
#include <gtest/gtest.h>

#include "AttributionIdSpineCombinerOptions.h"

using namespace ::pid::combiner;

class AttributionIdSpineFileCombinerTest : public testing::Test {
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
      std::vector<std::string>& spineIdContent,
      std::vector<std::string>& expectedOutput) {
    vectorStringToStream(dataContent, dataStream_);
    vectorStringToStream(spineIdContent, spineIdStream_);
    attributionIdSpineFileCombiner(dataStream_, spineIdStream_, outputStream_);
    validateOutputFile(expectedOutput);
  }

 protected:
  std::stringstream dataStream_;
  std::stringstream spineIdStream_;
  std::stringstream outputStream_;
};

// test basic padding
TEST_F(AttributionIdSpineFileCombinerTest, TestPublisherBasic) {
  std::vector<std::string> dataInput = {
      "id_,ad_id,timestamp,is_click,campaign_metadata",
      "id_1,1,100,1,1",
      "id_1,2,200,1,2",
      "id_2,1,200,1,3",
      "id_3,2,300,0,4",
      "id_4,1,400,0,5",
      "id_4,2,500,0,6"};
  std::vector<std::string> spineInput = {
      "AAAA,id_1", "BBBB,id_2", "CCCC,", "DDDD,", "EEEE,id_3", "FFFF,id_4"};
  std::vector<std::string> expectedOutput = {
      "id_,ad_ids,timestamps,is_click,campaign_metadata",
      "AAAA,[0,0,1,2],[0,0,100,200],[0,0,1,1],[0,0,1,2]",
      "BBBB,[0,0,0,1],[0,0,0,200],[0,0,0,1],[0,0,0,3]",
      "CCCC,[0,0,0,0],[0,0,0,0],[0,0,0,0],[0,0,0,0]",
      "DDDD,[0,0,0,0],[0,0,0,0],[0,0,0,0],[0,0,0,0]",
      "EEEE,[0,0,0,2],[0,0,0,300],[0,0,0,0],[0,0,0,4]",
      "FFFF,[0,0,1,2],[0,0,400,500],[0,0,0,0],[0,0,5,6]"};
  runTest(dataInput, spineInput, expectedOutput);
}

// test validation header with \r\n as new line
TEST_F(AttributionIdSpineFileCombinerTest, TestHeaderValidation) {
  std::vector<std::string> dataInput = {
      "id_,conversion_timestamp,conversion_value,conversion_metadata\r\nid_1,100,100,1"};
  std::vector<std::string> spineInput = {"AAAA,id_1"};
  std::vector<std::string> expectedOutput = {
      "id_,conversion_timestamps,conversion_values,conversion_metadata",
      "AAAA,[0,0,0,100],[0,0,0,100],[0,0,0,1]"};
  runTest(dataInput, spineInput, expectedOutput);
}

// test basic padding
TEST_F(AttributionIdSpineFileCombinerTest, TestPartnerBasic) {
  std::vector<std::string> dataInput = {
      "id_,conversion_timestamp,conversion_value,conversion_metadata",
      "id_1,100,100,1",
      "id_1,200,50,2",
      "id_2,200,10,3",
      "id_3,300,20,4",
      "id_4,400,0,5",
      "id_4,500,25,6"};
  std::vector<std::string> spineInput = {
      "AAAA,id_1", "BBBB,id_2", "CCCC,", "DDDD,", "EEEE,id_3", "FFFF,id_4"};
  std::vector<std::string> expectedOutput = {
      "id_,conversion_timestamps,conversion_values,conversion_metadata",
      "AAAA,[0,0,100,200],[0,0,100,50],[0,0,1,2]",
      "BBBB,[0,0,0,200],[0,0,0,10],[0,0,0,3]",
      "CCCC,[0,0,0,0],[0,0,0,0],[0,0,0,0]",
      "DDDD,[0,0,0,0],[0,0,0,0],[0,0,0,0]",
      "EEEE,[0,0,0,300],[0,0,0,20],[0,0,0,4]",
      "FFFF,[0,0,400,500],[0,0,0,25],[0,0,5,6]"};
  runTest(dataInput, spineInput, expectedOutput);
}

// test basic padding
TEST_F(AttributionIdSpineFileCombinerTest, TestPartnerPadding) {
  FLAGS_padding_size = 2;
  std::vector<std::string> dataInput = {
      "id_,conversion_timestamp,conversion_value,conversion_metadata",
      "id_1,100,100,1",
      "id_1,200,50,2",
      "id_2,200,10,3",
      "id_3,300,20,4",
      "id_4,400,0,5",
      "id_4,500,25,6"};
  std::vector<std::string> spineInput = {
      "AAAA,id_1", "BBBB,id_2", "CCCC,", "DDDD,", "EEEE,id_3", "FFFF,id_4"};
  std::vector<std::string> expectedOutput = {
      "id_,conversion_timestamps,conversion_values,conversion_metadata",
      "AAAA,[100,200],[100,50],[1,2]",
      "BBBB,[0,200],[0,10],[0,3]",
      "CCCC,[0,0],[0,0],[0,0]",
      "DDDD,[0,0],[0,0],[0,0]",
      "EEEE,[0,300],[0,20],[0,4]",
      "FFFF,[400,500],[0,25],[5,6]"};
  runTest(dataInput, spineInput, expectedOutput);
}

TEST_F(AttributionIdSpineFileCombinerTest, TestPartnerPaddingLimit) {
  FLAGS_padding_size = 4;
  std::vector<std::string> dataInput = {
      "id_,conversion_timestamp,conversion_value,conversion_metadata",
      "id_1,100,100,1",
      "id_1,200,50,2",
      "id_2,200,10,3",
      "id_3,300,20,4",
      "id_4,400,0,5",
      "id_4,500,25,6",
      "id_4,600,26,7",
      "id_4,700,27,8",
      "id_4,800,28,9",
      "id_4,900,29,10"};
  std::vector<std::string> spineInput = {
      "AAAA,id_1", "BBBB,id_2", "CCCC,", "DDDD,", "EEEE,id_3", "FFFF,id_4"};
  std::vector<std::string> expectedOutput = {
      "id_,conversion_timestamps,conversion_values,conversion_metadata",
      "AAAA,[0,0,100,200],[0,0,100,50],[0,0,1,2]",
      "BBBB,[0,0,0,200],[0,0,0,10],[0,0,0,3]",
      "CCCC,[0,0,0,0],[0,0,0,0],[0,0,0,0]",
      "DDDD,[0,0,0,0],[0,0,0,0],[0,0,0,0]",
      "EEEE,[0,0,0,300],[0,0,0,20],[0,0,0,4]",
      "FFFF,[400,500,600,700],[0,25,26,27],[5,6,7,8]"};

  runTest(dataInput, spineInput, expectedOutput);
}

TEST_F(AttributionIdSpineFileCombinerTest, TestPublisherPaddingLimit) {
  FLAGS_padding_size = 4;
  std::vector<std::string> dataInput = {
      "id_,ad_id,timestamp,is_click,campaign_metadata",
      "id_1,1,100,1,1",
      "id_1,2,200,1,2",
      "id_1,3,300,1,3",
      "id_1,4,400,1,4",
      "id_1,5,500,1,5",
      "id_2,1,200,1,3",
      "id_3,2,300,0,4",
      "id_4,1,400,0,5",
      "id_4,2,500,0,6"};
  std::vector<std::string> spineInput = {
      "AAAA,id_1", "BBBB,id_2", "CCCC,", "DDDD,", "EEEE,id_3", "FFFF,id_4"};
  std::vector<std::string> expectedOutput = {
      "id_,ad_ids,timestamps,is_click,campaign_metadata",
      "AAAA,[1,2,3,4],[100,200,300,400],[1,1,1,1],[1,2,3,4]",
      "BBBB,[0,0,0,1],[0,0,0,200],[0,0,0,1],[0,0,0,3]",
      "CCCC,[0,0,0,0],[0,0,0,0],[0,0,0,0],[0,0,0,0]",
      "DDDD,[0,0,0,0],[0,0,0,0],[0,0,0,0],[0,0,0,0]",
      "EEEE,[0,0,0,2],[0,0,0,300],[0,0,0,0],[0,0,0,4]",
      "FFFF,[0,0,1,2],[0,0,400,500],[0,0,0,0],[0,0,5,6]"};

  runTest(dataInput, spineInput, expectedOutput);
}
