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

#include <folly/Random.h>
#include "AttributionIdSpineCombinerOptions.h"
#include "fbpcs/data_processing/test_utils/FileIOTestUtils.h"

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
    auto randStart = folly::Random::secureRand64();
    std::string dataContentPath =
        "/tmp/AttributionIdSpineFileCombinerTestDataContent" +
        std::to_string(randStart);
    std::string spineIdContentPath =
        "/tmp/AttributionIdSpineFileCombinerTestSpineIdContent" +
        std::to_string(randStart);
    constexpr size_t kBufferedReaderChunkSize = 4096;
    data_processing::test_utils::writeVecToFile(dataContent, dataContentPath);
    data_processing::test_utils::writeVecToFile(
        spineIdContent, spineIdContentPath);
    auto dataReader = std::make_unique<fbpcf::io::FileReader>(dataContentPath);
    auto spineReader =
        std::make_unique<fbpcf::io::FileReader>(spineIdContentPath);
    auto bufferedDataReader = std::make_shared<fbpcf::io::BufferedReader>(
        std::move(dataReader), kBufferedReaderChunkSize);
    auto bufferedSpineReader = std::make_shared<fbpcf::io::BufferedReader>(
        std::move(spineReader), kBufferedReaderChunkSize);
    attributionIdSpineFileCombiner(
        bufferedDataReader,
        bufferedSpineReader,
        outputStream_,
        spineIdContentPath);
    bufferedDataReader->close();
    bufferedSpineReader->close();
    validateOutputFile(expectedOutput);
  }

 protected:
  std::stringstream outputStream_;
};

// test basic padding
TEST_F(AttributionIdSpineFileCombinerTest, TestPublisherBasic) {
  std::vector<std::string> dataInput = {
      "id_,ad_id,timestamp,is_click,campaign_metadata",
      "id_1,1,1656361100,1,1",
      "id_1,2,1656361200,1,2",
      "id_2,1,1656361200,1,3",
      "id_3,2,1656361300,0,4",
      "id_4,1,1656361400,0,5",
      "id_4,2,1656361500,0,6"};
  std::vector<std::string> spineInput = {
      "AAAA,id_1", "BBBB,id_2", "CCCC,", "DDDD,", "EEEE,id_3", "FFFF,id_4"};
  std::vector<std::string> expectedOutput = {
      "id_,ad_ids,timestamps,is_click,campaign_metadata",
      "AAAA,[0,0,1,2],[0,0,1656361100,1656361200],[0,0,1,1],[0,0,1,2]",
      "BBBB,[0,0,0,1],[0,0,0,1656361200],[0,0,0,1],[0,0,0,3]",
      "CCCC,[0,0,0,0],[0,0,0,0],[0,0,0,0],[0,0,0,0]",
      "DDDD,[0,0,0,0],[0,0,0,0],[0,0,0,0],[0,0,0,0]",
      "EEEE,[0,0,0,2],[0,0,0,1656361300],[0,0,0,0],[0,0,0,4]",
      "FFFF,[0,0,1,2],[0,0,1656361400,1656361500],[0,0,0,0],[0,0,5,6]"};
  runTest(dataInput, spineInput, expectedOutput);
}

// test validation header with \r\n as new line
TEST_F(AttributionIdSpineFileCombinerTest, TestHeaderValidation) {
  std::vector<std::string> dataInput = {
      "id_,conversion_timestamp,conversion_value,conversion_metadata\r\nid_1,1656361100,100,1"};
  std::vector<std::string> spineInput = {"AAAA,id_1"};
  std::vector<std::string> expectedOutput = {
      "id_,conversion_timestamps,conversion_values,conversion_metadata",
      "AAAA,[0,0,0,1656361100],[0,0,0,100],[0,0,0,1]"};
  runTest(dataInput, spineInput, expectedOutput);
}

// test basic padding
TEST_F(AttributionIdSpineFileCombinerTest, TestPartnerBasic) {
  std::vector<std::string> dataInput = {
      "id_,conversion_timestamp,conversion_value,conversion_metadata",
      "id_1,1656361100,100,1",
      "id_1,1656361200,50,2",
      "id_2,1656361200,10,3",
      "id_3,1656361300,20,4",
      "id_4,1656361400,0,5",
      "id_4,1656361500,25,6"};
  std::vector<std::string> spineInput = {
      "AAAA,id_1", "BBBB,id_2", "CCCC,", "DDDD,", "EEEE,id_3", "FFFF,id_4"};
  std::vector<std::string> expectedOutput = {
      "id_,conversion_timestamps,conversion_values,conversion_metadata",
      "AAAA,[0,0,1656361100,1656361200],[0,0,100,50],[0,0,1,2]",
      "BBBB,[0,0,0,1656361200],[0,0,0,10],[0,0,0,3]",
      "CCCC,[0,0,0,0],[0,0,0,0],[0,0,0,0]",
      "DDDD,[0,0,0,0],[0,0,0,0],[0,0,0,0]",
      "EEEE,[0,0,0,1656361300],[0,0,0,20],[0,0,0,4]",
      "FFFF,[0,0,1656361400,1656361500],[0,0,0,25],[0,0,5,6]"};
  runTest(dataInput, spineInput, expectedOutput);
}

// test basic padding
TEST_F(AttributionIdSpineFileCombinerTest, TestPartnerPadding) {
  FLAGS_padding_size = 2;
  std::vector<std::string> dataInput = {
      "id_,conversion_timestamp,conversion_value,conversion_metadata",
      "id_1,1656361100,100,1",
      "id_1,1656361200,50,2",
      "id_2,1656361200,10,3",
      "id_3,1656361300,20,4",
      "id_4,1656361400,0,5",
      "id_4,1656361500,25,6"};
  std::vector<std::string> spineInput = {
      "AAAA,id_1", "BBBB,id_2", "CCCC,", "DDDD,", "EEEE,id_3", "FFFF,id_4"};
  std::vector<std::string> expectedOutput = {
      "id_,conversion_timestamps,conversion_values,conversion_metadata",
      "AAAA,[1656361100,1656361200],[100,50],[1,2]",
      "BBBB,[0,1656361200],[0,10],[0,3]",
      "CCCC,[0,0],[0,0],[0,0]",
      "DDDD,[0,0],[0,0],[0,0]",
      "EEEE,[0,1656361300],[0,20],[0,4]",
      "FFFF,[1656361400,1656361500],[0,25],[5,6]"};
  runTest(dataInput, spineInput, expectedOutput);
}

// test basic padding for target_id and action_type
TEST_F(AttributionIdSpineFileCombinerTest, TestPublisherTargetIdBasic) {
  std::vector<std::string> dataInput = {
      "id_,ad_id,timestamp,is_click,campaign_metadata,target_id,action_type",
      "id_1,1,1656361100,1,1,54321,4",
      "id_1,2,1656361200,1,2,12345,4",
      "id_2,1,1656361200,1,3,12345,4",
      "id_3,2,1656361300,0,4,54321,4",
      "id_4,1,1656361400,0,5,54321,",
      "id_4,2,1656361500,0,6,,4"};
  std::vector<std::string> spineInput = {
      "AAAA,id_1", "BBBB,id_2", "CCCC,", "DDDD,", "EEEE,id_3", "FFFF,id_4"};
  std::vector<std::string> expectedOutput = {
      "id_,ad_ids,timestamps,is_click,campaign_metadata,target_id,action_type",
      "AAAA,[0,0,1,2],[0,0,1656361100,1656361200],[0,0,1,1],[0,0,1,2],[0,0,54321,12345],[0,0,4,4]",
      "BBBB,[0,0,0,1],[0,0,0,1656361200],[0,0,0,1],[0,0,0,3],[0,0,0,12345],[0,0,0,4]",
      "CCCC,[0,0,0,0],[0,0,0,0],[0,0,0,0],[0,0,0,0],[0,0,0,0],[0,0,0,0]",
      "DDDD,[0,0,0,0],[0,0,0,0],[0,0,0,0],[0,0,0,0],[0,0,0,0],[0,0,0,0]",
      "EEEE,[0,0,0,2],[0,0,0,1656361300],[0,0,0,0],[0,0,0,4],[0,0,0,54321],[0,0,0,4]",
      "FFFF,[0,0,1,2],[0,0,1656361400,1656361500],[0,0,0,0],[0,0,5,6],[0,0,54321,0],[0,0,0,4]"};
  runTest(dataInput, spineInput, expectedOutput);
}

// test validation header with \r\n as new line include target_id
TEST_F(AttributionIdSpineFileCombinerTest, TestTargetIdHeaderValidation) {
  std::vector<std::string> dataInput = {
      "id_,conversion_timestamp,conversion_value,conversion_metadata,conversion_target_id,conversion_action_type\r\nid_1,1656361100,100,1,,4"};
  std::vector<std::string> spineInput = {"AAAA,id_1"};
  std::vector<std::string> expectedOutput = {
      "id_,conversion_timestamps,conversion_values,conversion_metadata,conversion_target_id,conversion_action_type",
      "AAAA,[0,0,0,1656361100],[0,0,0,100],[0,0,0,1],[0,0,0,0],[0,0,0,4]"};
  runTest(dataInput, spineInput, expectedOutput);
}

// test basic padding for conversion_target_id and conversion_action_type
TEST_F(AttributionIdSpineFileCombinerTest, TestPartnerTargetIdBasic) {
  std::vector<std::string> dataInput = {
      "id_,conversion_timestamp,conversion_value,conversion_metadata,conversion_target_id,conversion_action_type",
      "id_1,1656361100,100,1,222222,4",
      "id_1,1656361200,50,2,,",
      "id_2,1656361200,10,3,11111,3",
      "id_3,1656361300,20,4,111,",
      "id_4,1656361400,0,5,0,0",
      "id_4,1656361500,25,6,12345,23"};
  std::vector<std::string> spineInput = {
      "AAAA,id_1", "BBBB,id_2", "CCCC,", "DDDD,", "EEEE,id_3", "FFFF,id_4"};
  std::vector<std::string> expectedOutput = {
      "id_,conversion_timestamps,conversion_values,conversion_metadata,conversion_target_id,conversion_action_type",
      "AAAA,[0,0,1656361100,1656361200],[0,0,100,50],[0,0,1,2],[0,0,222222,0],[0,0,4,0]",
      "BBBB,[0,0,0,1656361200],[0,0,0,10],[0,0,0,3],[0,0,0,11111],[0,0,0,3]",
      "CCCC,[0,0,0,0],[0,0,0,0],[0,0,0,0],[0,0,0,0],[0,0,0,0]",
      "DDDD,[0,0,0,0],[0,0,0,0],[0,0,0,0],[0,0,0,0],[0,0,0,0]",
      "EEEE,[0,0,0,1656361300],[0,0,0,20],[0,0,0,4],[0,0,0,111],[0,0,0,0]",
      "FFFF,[0,0,1656361400,1656361500],[0,0,0,25],[0,0,5,6],[0,0,0,12345],[0,0,0,23]"};
  runTest(dataInput, spineInput, expectedOutput);
}

// test missing either conversion_target_id or conversion_action_type
TEST_F(AttributionIdSpineFileCombinerTest, TestPartnerHeaderPadding) {
  FLAGS_padding_size = 2;
  std::vector<std::string> dataInput = {
      "id_,conversion_timestamp,conversion_value,conversion_metadata,conversion_action_type",
      "id_1,1656361100,100,1,4"};
  std::vector<std::string> spineInput = {"AAAA,id_1"};
  std::vector<std::string> expectedOutput = {
      "id_,conversion_timestamps,conversion_values,conversion_metadata,conversion_action_type",
      "AAAA,[0,1656361100],[0,100],[0,1],[0,4]"};
  ASSERT_DEATH(
      runTest(dataInput, spineInput, expectedOutput),
      ".*conversion_target_id.*");
}

// test missing either target_id or action_type
TEST_F(AttributionIdSpineFileCombinerTest, TestPublisherHeaderPadding) {
  std::vector<std::string> dataInput = {
      "id_,ad_id,timestamp,is_click,campaign_metadata,target_id",
      "id_1,1,1656361100,1,1,54321,4",
      "id_1,2,1656361200,1,2,12345,4"};
  std::vector<std::string> spineInput = {"AAAA,id_1"};
  std::vector<std::string> expectedOutput = {
      "id_,ad_ids,timestamps,is_click,campaign_metadata,target_id",
      "AAAA,[0,0,1,2],[0,0,1656361100,1656361200],[0,0,1,1],[0,0,1,2],[0,0,54321,12345]"};
  ASSERT_DEATH(runTest(dataInput, spineInput, expectedOutput), ".*target_id.*");
}

TEST_F(AttributionIdSpineFileCombinerTest, TestPartnerPaddingLimit) {
  FLAGS_padding_size = 4;
  std::vector<std::string> dataInput = {
      "id_,conversion_timestamp,conversion_value,conversion_metadata",
      "id_1,1656361100,100,1",
      "id_1,1656361200,50,2",
      "id_2,1656361200,10,3",
      "id_3,1656361300,20,4",
      "id_4,1656361400,0,5",
      "id_4,1656361500,25,6",
      "id_4,1656361600,26,7",
      "id_4,1656361700,27,8",
      "id_4,1656361800,28,9",
      "id_4,1656361900,29,10"};
  std::vector<std::string> spineInput = {
      "AAAA,id_1", "BBBB,id_2", "CCCC,", "DDDD,", "EEEE,id_3", "FFFF,id_4"};
  std::vector<std::string> expectedOutput = {
      "id_,conversion_timestamps,conversion_values,conversion_metadata",
      "AAAA,[0,0,1656361100,1656361200],[0,0,100,50],[0,0,1,2]",
      "BBBB,[0,0,0,1656361200],[0,0,0,10],[0,0,0,3]",
      "CCCC,[0,0,0,0],[0,0,0,0],[0,0,0,0]",
      "DDDD,[0,0,0,0],[0,0,0,0],[0,0,0,0]",
      "EEEE,[0,0,0,1656361300],[0,0,0,20],[0,0,0,4]",
      "FFFF,[1656361400,1656361500,1656361600,1656361700],[0,25,26,27],[5,6,7,8]"};

  runTest(dataInput, spineInput, expectedOutput);
}

TEST_F(AttributionIdSpineFileCombinerTest, TestPublisherPaddingLimit) {
  FLAGS_padding_size = 4;
  std::vector<std::string> dataInput = {
      "id_,ad_id,timestamp,is_click,campaign_metadata",
      "id_1,1,1656361100,1,1",
      "id_1,2,1656361200,1,2",
      "id_1,3,1656361300,1,3",
      "id_1,4,1656361400,1,4",
      "id_1,5,1656361500,1,5",
      "id_2,1,1656361200,1,3",
      "id_3,2,1656361300,0,4",
      "id_4,1,1656361400,0,5",
      "id_4,2,1656361500,0,6"};
  std::vector<std::string> spineInput = {
      "AAAA,id_1", "BBBB,id_2", "CCCC,", "DDDD,", "EEEE,id_3", "FFFF,id_4"};
  std::vector<std::string> expectedOutput = {
      "id_,ad_ids,timestamps,is_click,campaign_metadata",
      "AAAA,[1,2,3,4],[1656361100,1656361200,1656361300,1656361400],[1,1,1,1],[1,2,3,4]",
      "BBBB,[0,0,0,1],[0,0,0,1656361200],[0,0,0,1],[0,0,0,3]",
      "CCCC,[0,0,0,0],[0,0,0,0],[0,0,0,0],[0,0,0,0]",
      "DDDD,[0,0,0,0],[0,0,0,0],[0,0,0,0],[0,0,0,0]",
      "EEEE,[0,0,0,2],[0,0,0,1656361300],[0,0,0,0],[0,0,0,4]",
      "FFFF,[0,0,1,2],[0,0,1656361400,1656361500],[0,0,0,0],[0,0,5,6]"};

  runTest(dataInput, spineInput, expectedOutput);
}

TEST_F(AttributionIdSpineFileCombinerTest, TestMultiKeyWithMaxOne) {
  FLAGS_padding_size = 4;
  std::vector<std::string> dataInput = {
      "id_email,id_phone,id_fn,ad_id,timestamp,is_click,campaign_metadata",
      "email1,,,1,1656361100,1,1",
      "email1,phone1,,2,1656361200,1,2",
      "email1,,fn1,3,1656361300,1,3",
      "email1,phone1,fn1,4,1656361400,1,4",
      "email1,phone1,fn1,5,1656361500,1,5",
      ",phone2,,1,1656361200,1,3",
      ",phone2,fn2,2,1656361300,0,4",
      ",,fn3,1,1656361400,0,5",
      ",phone3,fn3,2,1656361500,0,6"};
  std::vector<std::string> spineInput = {
      "AAAA,email1",
      "BBBB,phone2",
      "CCCC,fn3",
      "DDDD,NA",
      "EEEE,",
      "FFFF,phone3"};
  std::vector<std::string> expectedOutput = {
      "id_,ad_ids,timestamps,is_click,campaign_metadata",
      "AAAA,[1,2,3,4],[1656361100,1656361200,1656361300,1656361400],[1,1,1,1],[1,2,3,4]",
      "BBBB,[0,0,1,2],[0,0,1656361200,1656361300],[0,0,1,0],[0,0,3,4]",
      "CCCC,[0,0,0,1],[0,0,0,1656361400],[0,0,0,0],[0,0,0,5]",
      "DDDD,[0,0,0,0],[0,0,0,0],[0,0,0,0],[0,0,0,0]",
      "EEEE,[0,0,0,0],[0,0,0,0],[0,0,0,0],[0,0,0,0]",
      "FFFF,[0,0,0,2],[0,0,0,1656361500],[0,0,0,0],[0,0,0,6]"};

  runTest(dataInput, spineInput, expectedOutput);
}

TEST_F(AttributionIdSpineFileCombinerTest, TestMultiKeyWithMaxTwo) {
  FLAGS_padding_size = 5;
  std::vector<std::string> dataInput = {
      "id_email,id_phone,id_fn,ad_id,timestamp,is_click,campaign_metadata",
      "email1,phone1,fn1,4,1656361400,1,4",
      "email1,,,1,1656361100,1,1",
      "email1,phone1,,2,1656361200,1,2",
      "email1,,fn1,3,1656361300,1,3",
      "email1,phone1,fn1,5,1656361500,1,5",
      ",phone2,fn2,2,1656361300,0,4",
      ",phone2,,1,1656361200,1,3",
      ",phone3,fn3,2,1656361500,0,6",
      ",,fn3,1,1656361400,0,5"};
  std::vector<std::string> spineInput = {
      "AAAA,email1,phone1",
      "DDDD,phone2,fn2",
      "FFFF,phone3,fn3",
      "HHHH,NA",
      "IIII,"};
  std::vector<std::string> expectedOutput = {
      "id_,ad_ids,timestamps,is_click,campaign_metadata",
      "AAAA,[4,1,2,3,5],[1656361400,1656361100,1656361200,1656361300,1656361500],[1,1,1,1,1],[4,1,2,3,5]",
      "DDDD,[0,0,0,2,1],[0,0,0,1656361300,1656361200],[0,0,0,0,1],[0,0,0,4,3]",
      "FFFF,[0,0,0,2,1],[0,0,0,1656361500,1656361400],[0,0,0,0,0],[0,0,0,6,5]",
      "HHHH,[0,0,0,0,0],[0,0,0,0,0],[0,0,0,0,0],[0,0,0,0,0]",
      "IIII,[0,0,0,0,0],[0,0,0,0,0],[0,0,0,0,0],[0,0,0,0,0]"};
  FLAGS_max_id_column_cnt = 2;
  runTest(dataInput, spineInput, expectedOutput);
}

TEST_F(AttributionIdSpineFileCombinerTest, TestMultiKeyWithMaxThree) {
  FLAGS_padding_size = 5;
  std::vector<std::string> dataInput = {
      "id_email,id_phone,id_fn,ad_id,timestamp,is_click,campaign_metadata",
      "email1,phone1,fn1,4,1656361400,1,4",
      "email1,,,1,1656361100,1,1",
      "email1,phone1,,2,1656361200,1,2",
      "email1,,fn1,3,1656361300,1,3",
      "email1,phone1,fn1,5,1656361500,1,5",
      ",phone2,fn2,2,1656361300,0,4",
      ",phone2,,1,1656361200,1,3",
      ",phone3,fn3,2,1656361500,0,6",
      ",,fn3,1,1656361400,0,5"};
  std::vector<std::string> spineInput = {
      "AAAA,email1,phone1,fn1",
      "DDDD,phone2,fn2",
      "FFFF,phone3,fn3",
      "HHHH,NA",
      "IIII,"};
  std::vector<std::string> expectedOutput = {
      "id_,ad_ids,timestamps,is_click,campaign_metadata",
      "AAAA,[4,1,2,3,5],[1656361400,1656361100,1656361200,1656361300,1656361500],[1,1,1,1,1],[4,1,2,3,5]",
      "DDDD,[0,0,0,2,1],[0,0,0,1656361300,1656361200],[0,0,0,0,1],[0,0,0,4,3]",
      "FFFF,[0,0,0,2,1],[0,0,0,1656361500,1656361400],[0,0,0,0,0],[0,0,0,6,5]",
      "HHHH,[0,0,0,0,0],[0,0,0,0,0],[0,0,0,0,0],[0,0,0,0,0]",
      "IIII,[0,0,0,0,0],[0,0,0,0,0],[0,0,0,0,0],[0,0,0,0,0]"};
  FLAGS_max_id_column_cnt = 3;
  runTest(dataInput, spineInput, expectedOutput);
}

TEST_F(AttributionIdSpineFileCombinerTest, TestMultiKeyWithMaxFour) {
  FLAGS_padding_size = 5;
  std::vector<std::string> dataInput = {
      "id_email,id_phone,id_fn,ad_id,timestamp,is_click,campaign_metadata",
      "email1,phone1,fn1,4,1656361400,1,4",
      "email1,,,1,1656361100,1,1",
      "email1,phone1,,2,1656361200,1,2",
      "email1,,fn1,3,1656361300,1,3",
      "email1,phone1,fn1,5,1656361500,1,5",
      ",phone2,fn2,2,1656361300,0,4",
      ",phone2,,1,1656361200,1,3",
      ",phone3,fn3,2,1656361500,0,6",
      ",,fn3,1,1656361400,0,5"};
  std::vector<std::string> spineInput = {
      "AAAA,email1,phone1,fn1",
      "DDDD,phone2,fn2",
      "FFFF,phone3,fn3",
      "HHHH,NA",
      "IIII,"};
  std::vector<std::string> expectedOutput = {
      "id_,ad_ids,timestamps,is_click,campaign_metadata",
      "AAAA,[4,1,2,3,5],[1656361400,1656361100,1656361200,1656361300,1656361500],[1,1,1,1,1],[4,1,2,3,5]",
      "DDDD,[0,0,0,2,1],[0,0,0,1656361300,1656361200],[0,0,0,0,1],[0,0,0,4,3]",
      "FFFF,[0,0,0,2,1],[0,0,0,1656361500,1656361400],[0,0,0,0,0],[0,0,0,6,5]",
      "HHHH,[0,0,0,0,0],[0,0,0,0,0],[0,0,0,0,0],[0,0,0,0,0]",
      "IIII,[0,0,0,0,0],[0,0,0,0,0],[0,0,0,0,0],[0,0,0,0,0]"};
  FLAGS_max_id_column_cnt = 4;
  runTest(dataInput, spineInput, expectedOutput);
}
