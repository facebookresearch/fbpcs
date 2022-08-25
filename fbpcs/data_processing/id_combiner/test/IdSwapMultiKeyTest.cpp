/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "../IdSwapMultiKey.h"

#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <fstream>

#include <folly/Random.h>
#include <gflags/gflags.h>
#include <gtest/gtest.h>

#include <folly/logging/xlog.h>
#include "fbpcs/data_processing/test_utils/FileIOTestUtils.h"

class IdSwapMultiKeyTest : public testing::Test {
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
      std::vector<std::string>& expectedOutput,
      std::int32_t maxIdColumnCnt,
      bool isPublisherLift = false) {
    // Execute the union pid combiner with the pre-created files
    auto randStart = folly::Random::secureRand64();
    std::string dataInputPath =
        "/tmp/AttributionIdSpineFileCombinerTestDataPath" +
        std::to_string(randStart);
    std::string spineInputPath =
        "/tmp/AttributionIdSpineFileCombinerTestSpineInputPath" +
        std::to_string(randStart);
    constexpr size_t kBufferedReaderChunkSize = 4096;
    data_processing::test_utils::writeVecToFile(dataInput, dataInputPath);
    data_processing::test_utils::writeVecToFile(spineInput, spineInputPath);
    auto dataReader = std::make_unique<fbpcf::io::FileReader>(dataInputPath);
    auto spineReader = std::make_unique<fbpcf::io::FileReader>(spineInputPath);
    auto bufferedDataReader = std::make_shared<fbpcf::io::BufferedReader>(
        std::move(dataReader), kBufferedReaderChunkSize);
    auto bufferedSpineReader = std::make_shared<fbpcf::io::BufferedReader>(
        std::move(spineReader), kBufferedReaderChunkSize);
    std::string headerLine = bufferedDataReader->readLine();
    pid::combiner::idSwapMultiKey(
        bufferedDataReader,
        bufferedSpineReader,
        outputStream_,
        maxIdColumnCnt,
        headerLine,
        spineInputPath,
        isPublisherLift);
    bufferedDataReader->close();
    bufferedSpineReader->close();
    validateOutputContent(expectedOutput);
  }

 protected:
  std::stringstream outputStream_;
};

// Valid spine with some amount of overlap for publisher
// As this is publisher data the opp_flag flag needs to be created in the
// program itself
TEST_F(IdSwapMultiKeyTest, ValidSpinePublisher) {
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
      "BBBB,0,0",
      "CCCC,150,0",
      "DDDD,200,0",
      "EEEE,0,0",
      "FFFF,0,0",
  };
  int32_t maxIdColumnCnt = 1;
  runTest(dataInput, spineInput, expectedOutput, maxIdColumnCnt);
}

// The only reason that this is a separate test is because we insert a column
// at the end and then check where the opportunity_timestamp column exists.
// This led to a bug since we threw a std::out_of_range in a real test.
TEST_F(IdSwapMultiKeyTest, ValidSpinePublisherTimestampLastColumn) {
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
      "BBBB,0,0",
      "CCCC,0,150",
      "DDDD,0,200",
      "EEEE,0,0",
      "FFFF,0,0",
  };
  int32_t maxIdColumnCnt = 1;
  runTest(dataInput, spineInput, expectedOutput, maxIdColumnCnt);
}

// Valid spine with some amount of overlap for partner
// No opp_flag flag needed at the output level
TEST_F(IdSwapMultiKeyTest, ValidSpinePartner) {
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
      "CCCC,0,0",
      "DDDD,0,0",
      "EEEE,375,300",
      "FFFF,400,400"};
  int32_t maxIdColumnCnt = 1;
  runTest(dataInput, spineInput, expectedOutput, maxIdColumnCnt);
}

// Test with IdColumnIndex not at 0
TEST_F(IdSwapMultiKeyTest, IdColumnIndexNotZero) {
  std::vector<std::string> dataInput = {
      "event_timestamp,id_,value",
      "125,123,100",
      "200,111,200",
      "375,222,300",
      "400,333,400"};
  std::vector<std::string> spineInput = {
      "AAAA,123", "BBBB,111", "CCCC,", "DDDD,", "EEEE,222", "FFFF,333"};
  std::vector<std::string> expectedOutput = {
      "id_,event_timestamp,value",
      "AAAA,125,100",
      "BBBB,200,200",
      "CCCC,0,0",
      "DDDD,0,0",
      "EEEE,375,300",
      "FFFF,400,400"};
  int32_t maxIdColumnCnt = 1;
  runTest(dataInput, spineInput, expectedOutput, maxIdColumnCnt);
}

// Test with IdColumnIndex at last col
TEST_F(IdSwapMultiKeyTest, IdColumnIndexLastCol) {
  std::vector<std::string> dataInput = {
      "event_timestamp,value,id_",
      "125,100,123",
      "200,200,111",
      "375,300,222",
      "400,400,333"};
  std::vector<std::string> spineInput = {
      "AAAA,123", "BBBB,111", "CCCC,", "DDDD,", "EEEE,222", "FFFF,333"};
  std::vector<std::string> expectedOutput = {
      "id_,event_timestamp,value",
      "AAAA,125,100",
      "BBBB,200,200",
      "CCCC,0,0",
      "DDDD,0,0",
      "EEEE,375,300",
      "FFFF,400,400"};
  int32_t maxIdColumnCnt = 1;
  runTest(dataInput, spineInput, expectedOutput, maxIdColumnCnt);
}

// Id missing in spine
// We'd expect an error to be thrown here
// Some mismatch between pid service output and dataFile
TEST_F(IdSwapMultiKeyTest, MissingPrivateIdsSpine) {
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

  auto randStart = folly::Random::secureRand64();
  std::string dataInputPath =
      "/tmp/AttributionIdSpineFileCombinerTestDataPath" +
      std::to_string(randStart);
  std::string spineInputPath =
      "/tmp/AttributionIdSpineFileCombinerTestSpineInputPath" +
      std::to_string(randStart);
  constexpr size_t kBufferedReaderChunkSize = 4096;
  data_processing::test_utils::writeVecToFile(dataInput, dataInputPath);
  data_processing::test_utils::writeVecToFile(spineInput, spineInputPath);
  auto dataReader = std::make_unique<fbpcf::io::FileReader>(dataInputPath);
  auto spineReader = std::make_unique<fbpcf::io::FileReader>(spineInputPath);
  auto bufferedDataReader = std::make_shared<fbpcf::io::BufferedReader>(
      std::move(dataReader), kBufferedReaderChunkSize);
  auto bufferedSpineReader = std::make_shared<fbpcf::io::BufferedReader>(
      std::move(spineReader), kBufferedReaderChunkSize);
  int32_t maxIdColumnCnt = 1;
  std::string headerLine = bufferedDataReader->readLine();
  ASSERT_DEATH(
      pid::combiner::idSwapMultiKey(
          bufferedDataReader,
          bufferedSpineReader,
          outputStream_,
          maxIdColumnCnt,
          headerLine,
          spineInputPath),
      "ID is missing in the spineID file");
  bufferedDataReader->close();
  bufferedSpineReader->close();
}

// Spine id contains an id_ that doesn't exist in data
TEST_F(IdSwapMultiKeyTest, MissingPrivateIdsInData) {
  std::vector<std::string> dataInput = {
      "id_,event_timestamp,value", "111,200,200", "222,375,300", "333,400,400"};
  std::vector<std::string> spineInput = {
      "BBBB,111", "CCCC,", "DDDD,444", "EEEE,222", "FFFF,333"};
  std::vector<std::string> expectedOutput = {
      "id_,event_timestamp,value",
      "BBBB,200,200",
      "CCCC,0,0",
      "DDDD,0,0",
      "EEEE,375,300",
      "FFFF,400,400"};

  int32_t maxIdColumnCnt = 1;
  runTest(dataInput, spineInput, expectedOutput, maxIdColumnCnt);
}

// Rows with duplicate ids
// We would expect the data to flow down as the same
TEST_F(IdSwapMultiKeyTest, DuplicateIdsData) {
  std::vector<std::string> dataInput = {
      "id_,opportunity_timestamp,test_flag",
      "123,100,1",
      "123,120,1",
      "456,150,0",
      "456,160,1",
      "789,200,0"};
  std::vector<std::string> spineInput = {
      "AAAA,123", "BBBB,", "CCCC,456", "DDDD,789", "EEEE,", "FFFF,"};
  std::vector<std::string> expectedOutput = {
      "id_,opportunity_timestamp,test_flag",
      "AAAA,100,1",
      "AAAA,120,1",
      "BBBB,0,0",
      "CCCC,150,0",
      "CCCC,160,1",
      "DDDD,200,0",
      "EEEE,0,0",
      "FFFF,0,0",
  };
  int32_t maxIdColumnCnt = 1;
  runTest(dataInput, spineInput, expectedOutput, maxIdColumnCnt);
}

// Aggregate rows with duplicate ids
// We would expect the data to flow down as the same
TEST_F(IdSwapMultiKeyTest, AggregateDuplicateIdsData) {
  std::vector<std::string> dataInput = {
      "id_,opportunity_timestamp,test_flag,num_impressions,num_clicks,total_spend,breakdown_id,unregistered",
      "123,100,1,1,3,200,0,2",
      "123,120,1,2,4,300,1,3",
      "456,150,0,2,2,150,0,4",
      "456,160,0,3,3,250,1,5",
      "789,200,0,2,2,100,0,6"};
  std::vector<std::string> spineInput = {
      "AAAA,123", "BBBB,", "CCCC,456", "DDDD,789", "EEEE,", "FFFF,"};
  std::vector<std::string> expectedOutput = {
      "id_,opportunity_timestamp,test_flag,num_impressions,num_clicks,total_spend,breakdown_id,unregistered",
      "AAAA,100,1,3,7,500,1,2",
      "BBBB,0,0,0,0,0,0,0",
      "CCCC,150,0,5,5,400,1,4",
      "DDDD,200,0,2,2,100,0,6",
      "EEEE,0,0,0,0,0,0,0",
      "FFFF,0,0,0,0,0,0,0",
  };
  int32_t maxIdColumnCnt = 1;
  runTest(dataInput, spineInput, expectedOutput, maxIdColumnCnt, true);
}

// Fail when non-id column cannot be casted to int
TEST_F(IdSwapMultiKeyTest, NonIntCastableColumns) {
  std::vector<std::string> header{
      "id_", "opportunity_timestamp", "test_flag", "num_impressions"};
  std::vector<std::vector<std::string>> dRows{
      std::vector<std::string>{"abc", "0", "1"},
  };
  ASSERT_DEATH(
      pid::combiner::aggregateLiftNonIdColumns(header, dRows),
      "Error: Exception caught during casting string to int.");
}

// Fail when non-id column cannot be casted to int
TEST_F(IdSwapMultiKeyTest, MismatchBetweenHeaderAndRows) {
  std::vector<std::string> header{
      "id_", "opportunity_timestamp", "test_flag", "num_impressions"};
  std::vector<std::vector<std::string>> dRows{
      std::vector<std::string>{"111", "0"},
  };
  ASSERT_DEATH(
      pid::combiner::aggregateLiftNonIdColumns(header, dRows),
      "Error: number of non-id columns not consistent with header.");
}

// three id keys but only single key would be used
TEST_F(IdSwapMultiKeyTest, MultiKeyWithMaxOne) {
  std::vector<std::string> dataInput = {
      "id_,id_1,id_2,opportunity_timestamp,test_flag",
      "123,111,999,100,1",
      "123,222,888,120,1",
      "456,333,777,150,0",
      "456,333,777,160,1",
      "789,333,666,200,0",
      "789,555,,200,0",
      ",789,,200,0"};
  std::vector<std::string> spineInput = {
      "AAAA,123", "BBBB,", "CCCC,456", "DDDD,789", "EEEE,", "FFFF,"};
  std::vector<std::string> expectedOutput = {
      "id_,opportunity_timestamp,test_flag",
      "AAAA,100,1",
      "AAAA,120,1",
      "BBBB,0,0",
      "CCCC,150,0",
      "CCCC,160,1",
      "DDDD,200,0",
      "DDDD,200,0",
      "DDDD,200,0",
      "EEEE,0,0",
      "FFFF,0,0",
  };
  int32_t maxIdColumnCnt = 1;
  runTest(dataInput, spineInput, expectedOutput, maxIdColumnCnt);
}

// three id keys but two keys would be used
TEST_F(IdSwapMultiKeyTest, MultiKeyWithMaxTwo) {
  std::vector<std::string> dataInput = {
      "id_,id_1,id_2,opportunity_timestamp,test_flag",
      "123,111,999,100,1",
      "123,222,888,120,1",
      "456,333,777,150,0",
      "456,333,777,160,1",
      "789,333,666,170,0",
      "789,,555,180,0",
      ",,789,190,0"};
  std::vector<std::string> spineInput = {
      "AAAA,123,111", "CCCC,456,333", "EEEE,789,555", "GGGG,NA", "HHHH,"};
  std::vector<std::string> expectedOutput = {
      "id_,opportunity_timestamp,test_flag",
      "AAAA,100,1",
      "AAAA,120,1",
      "CCCC,150,0",
      "CCCC,160,1",
      "EEEE,170,0",
      "EEEE,180,0",
      "EEEE,190,0",
      "GGGG,0,0",
      "HHHH,0,0",
  };
  int32_t maxIdColumnCnt = 2;
  runTest(dataInput, spineInput, expectedOutput, maxIdColumnCnt);
}

// three id keys and all the keys would be used
TEST_F(IdSwapMultiKeyTest, MultiKeyWithMaxThree) {
  std::vector<std::string> dataInput = {
      "id_,id_1,id_2,opportunity_timestamp,test_flag",
      "123,111,999,100,1",
      "123,222,888,120,1",
      "456,333,777,150,0",
      "456,333,777,160,1",
      "789,333,666,200,0",
      "789,555,,200,0",
      ",789,,200,0"};
  std::vector<std::string> spineInput = {
      "AAAA,123,111,999",
      "CCCC,456,333,777",
      "EEEE,789,555",
      "GGGG,NA",
      "HHHH,"};
  std::vector<std::string> expectedOutput = {
      "id_,opportunity_timestamp,test_flag",
      "AAAA,100,1",
      "AAAA,120,1",
      "CCCC,150,0",
      "CCCC,160,1",
      "EEEE,200,0",
      "EEEE,200,0",
      "EEEE,200,0",
      "GGGG,0,0",
      "HHHH,0,0",
  };
  int32_t maxIdColumnCnt = 3;
  runTest(dataInput, spineInput, expectedOutput, maxIdColumnCnt);
}

// three id keys and but maximum is specified as four
TEST_F(IdSwapMultiKeyTest, MultiKeyWithMaxFour) {
  std::vector<std::string> dataInput = {
      "id_,id_1,id_2,opportunity_timestamp,test_flag",
      "123,111,999,100,1",
      "123,222,888,120,1",
      "456,333,777,150,0",
      "456,333,777,160,1",
      "789,333,666,200,0",
      "789,555,,200,0",
      ",,789,200,0"};
  std::vector<std::string> spineInput = {
      "AAAA,123,111,999",
      "CCCC,456,333,777",
      "EEEE,789,555",
      "GGGG,NA",
      "HHHH,"};
  std::vector<std::string> expectedOutput = {
      "id_,opportunity_timestamp,test_flag",
      "AAAA,100,1",
      "AAAA,120,1",
      "CCCC,150,0",
      "CCCC,160,1",
      "EEEE,200,0",
      "EEEE,200,0",
      "EEEE,200,0",
      "GGGG,0,0",
      "HHHH,0,0",
  };
  int32_t maxIdColumnCnt = 4;
  runTest(dataInput, spineInput, expectedOutput, maxIdColumnCnt);
}

TEST_F(IdSwapMultiKeyTest, MultiKeyWithRandomColumnOrder) {
  std::vector<std::string> dataInput = {
      "id_,opportunity_timestamp,id_1,id_2,test_flag",
      "123,100,111,999,1",
      "123,120,222,888,1",
      "456,150,333,777,0",
      "456,160,333,777,1",
      "789,200,333,666,0",
      "789,200,,555,0",
      ",200,,789,0"};
  std::vector<std::string> spineInput = {
      "AAAA,123,111,999",
      "CCCC,456,333,777",
      "EEEE,789,555",
      "GGGG,NA",
      "HHHH,"};
  std::vector<std::string> expectedOutput = {
      "id_,opportunity_timestamp,test_flag",
      "AAAA,100,1",
      "AAAA,120,1",
      "CCCC,150,0",
      "CCCC,160,1",
      "EEEE,200,0",
      "EEEE,200,0",
      "EEEE,200,0",
      "GGGG,0,0",
      "HHHH,0,0",
  };
  int32_t maxIdColumnCnt = 3;
  runTest(dataInput, spineInput, expectedOutput, maxIdColumnCnt);
}
