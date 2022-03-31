/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "../LiftIdSpineFileCombiner.h"

#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <fstream>

#include <gflags/gflags.h>
#include <gtest/gtest.h>

using namespace ::pid;
using namespace std::chrono;

class LiftIdSpineFileCombinerTest : public testing::Test {
 public:
  void writeToFile(
      std::string& filePath,
      std::vector<std::string>& stringData) {
    // If file already exists, another test is running in parallel
    // within the same microsecond. Wait till its cleaned up
    while (std::filesystem::exists(filePath)) {
    };

    std::ofstream fileStream(filePath);
    for (auto const& x : stringData) {
      fileStream << x << '\n';
    }
  }

  void setUpFiles(
      std::vector<std::string>& dataContent,
      std::vector<std::string>& idSpineContent) {
    // Generate unique filenames for each run to avoid  race condition
    int64_t timestamp =
        duration_cast<nanoseconds>(system_clock::now().time_since_epoch())
            .count();

    dataFilePath_ = std::to_string(timestamp) + "data.txt";
    spineFilePath_ = std::to_string(timestamp) + "spine.txt";
    outputFilePath_ = std::to_string(timestamp) + "output.txt";

    // Create, open and write to the data and spine file
    writeToFile(dataFilePath_, dataContent);
    writeToFile(spineFilePath_, idSpineContent);
  }

  void validateOutputFile(std::vector<std::string>& expectedOutput) {
    // Validate the output with what is expected
    std::ifstream outputFile{outputFilePath_};
    uint64_t lineIndex = 0;
    std::string outputString;
    while (getline(outputFile, outputString)) {
      EXPECT_EQ(outputString, expectedOutput.at(lineIndex));
      ++lineIndex;
    }
    // Should not be any extra entries any side
    EXPECT_EQ(lineIndex, expectedOutput.size());
  }

  void runTest(
      std::vector<std::string>& dataContent,
      std::vector<std::string>& idSpineContent,
      std::vector<std::string>& expectedOutput) {
    setUpFiles(dataContent, idSpineContent);
    // Execute the union pid combiner with the pre-created files
    std::filesystem::path dataPath{dataFilePath_};
    std::filesystem::path spinePath{spineFilePath_};
    std::filesystem::path outPath{outputFilePath_};
    LiftIdSpineFileCombiner combiner{dataPath, spinePath, outPath, "/tmp/"};
    combiner.combineFile();
    validateOutputFile(expectedOutput);
  }

  void TearDown() override {
    // Call base TearDown
    Test::TearDown();
    // All done, cleanup files now
    std::remove(dataFilePath_.c_str());
    std::remove(spineFilePath_.c_str());
    std::remove(outputFilePath_.c_str());
  }

 protected:
  std::string dataFilePath_;
  std::string spineFilePath_;
  std::string outputFilePath_;
};

// generic header is missing "id_" column
TEST_F(LiftIdSpineFileCombinerTest, InvalidHeader) {
  std::vector<std::string> lines = {
      "aaa,bbb,ccc", "123,456,789", "111,222,333"};
  std::vector<std::string> spineInput = {
      "AAAA,123", "BBBB,", "CCCC,456", "DDDD,789", "EEEE,", "FFFF,"};
  setUpFiles(lines, spineInput);
  // Execute the union pid combiner with the pre-created files
  std::filesystem::path dataPath{dataFilePath_};
  std::filesystem::path spinePath{spineFilePath_};
  std::filesystem::path outPath{outputFilePath_};
  LiftIdSpineFileCombiner combiner{dataPath, spinePath, outPath, "/tmp/"};

  ASSERT_DEATH(combiner.combineFile(), "Invalid headers for dataset");
}

// partner header is missing "event_timestamp" column
TEST_F(LiftIdSpineFileCombinerTest, InvalidPartnerHeader) {
  std::vector<std::string> dataInput = {
      "id_,value", "123,125", "111,200", "222,375", "333,400"};
  std::vector<std::string> spineInput = {
      "AAAA,123", "BBBB,111", "CCCC,", "DDDD,", "EEEE,222", "FFFF,333"};
  setUpFiles(dataInput, spineInput);
  // Execute the union pid combiner with the pre-created files
  std::filesystem::path dataPath{dataFilePath_};
  std::filesystem::path spinePath{spineFilePath_};
  std::filesystem::path outPath{outputFilePath_};
  LiftIdSpineFileCombiner combiner{dataPath, spinePath, outPath, "/tmp/"};

  ASSERT_DEATH(combiner.combineFile(), "Invalid headers for dataset");
}

TEST_F(LiftIdSpineFileCombinerTest, RowLengthMismatch) {
  std::vector<std::string> lines = {
      "id_,event_timestamp,value,ccc", "123,456,789,4", "111,222,333"};
  std::vector<std::string> spineInput = {
      "AAAA,123", "BBBB,", "CCCC,456", "DDDD,789", "EEEE,", "FFFF,"};
  setUpFiles(lines, spineInput);
  // Execute the union pid combiner with the pre-created files
  std::filesystem::path dataPath{dataFilePath_};
  std::filesystem::path spinePath{spineFilePath_};
  std::filesystem::path outPath{outputFilePath_};
  LiftIdSpineFileCombiner combiner{dataPath, spinePath, outPath, "/tmp/"};

  // TODO T86923630: Uncomment this once data validation supports hashed ids
  // This assertion is temporary disabled because there's a workaround in the
  // source code to disable data validation
  // ASSERT_DEATH(
  //     combiner.combineFile(),
  //     ".*Row at index <2> and header sizes mismatch. Row size is 3 and header
  //     size is .*");
}

TEST_F(LiftIdSpineFileCombinerTest, ParseFailure) {
  std::vector<std::string> dataInput = {
      "id_,event_timestamp,value",
      "abc,cdf,100",
      "111,200gh,200",
      "222,375,300",
      "333,400,400"};
  std::vector<std::string> spineInput = {
      "AAAA,123", "BBBB,", "CCCC,456", "DDDD,789", "EEEE,", "FFFF,"};
  setUpFiles(dataInput, spineInput);
  // Execute the union pid combiner with the pre-created files
  std::filesystem::path dataPath{dataFilePath_};
  std::filesystem::path spinePath{spineFilePath_};
  std::filesystem::path outPath{outputFilePath_};
  LiftIdSpineFileCombiner combiner{dataPath, spinePath, outPath, "/tmp/"};

  // TODO T86923630: Uncomment this once data validation supports hashed ids
  // This assertion is temporary disabled because there's a workaround in the
  // source code to disable data validation
  // ASSERT_DEATH(
  //     combiner.combineFile(),
  //     ".* in input file is not a number. Please validate your input.*");
}

// Valid spine with some amount of overlap for publisher
// As this is publisher data the opp_flag flag needs to be created in the
// program itself
TEST_F(LiftIdSpineFileCombinerTest, ValidSpinePublisher) {
  // FLAGS_sort_strategy is "sort" by default
  // So testing keep_original will require over write it.
  FLAGS_sort_strategy = "keep_original";
  std::vector<std::string> dataInput = {
      "id_,opportunity_timestamp,test_flag",
      "123,100,1",
      "456,150,0",
      "789,200,0"};
  std::vector<std::string> spineInput = {
      "FFFF,", "EEEE,", "DDDD,789", "CCCC,456", "BBBB,", "AAAA,123"};
  std::vector<std::string> expectedOutput = {
      "id_,opportunity_timestamp,opportunity,test_flag",
      "FFFF,0,0,0",
      "EEEE,0,0,0",
      "DDDD,200,1,0",
      "CCCC,150,1,0",
      "BBBB,0,0,0",
      "AAAA,100,1,1"};
  runTest(dataInput, spineInput, expectedOutput);
}

TEST_F(LiftIdSpineFileCombinerTest, ValidSortedSpinePublisher) {
  std::vector<std::string> dataInput = {
      "id_,opportunity_timestamp,test_flag",
      "aaa,100,1",
      "bbb,150,0",
      "ccc,200,0"};
  std::vector<std::string> spineInput = {
      "1,aaa", "2,", "3,bbb", "10,ccc", "100,", "123,"};
  // We tread id_ column as a string
  // so the sort will based on lexicographical order.
  std::vector<std::string> expectedOutput = {
      "id_,opportunity_timestamp,opportunity,test_flag",
      "1,100,1,1",
      "10,200,1,0",
      "100,0,0,0",
      "123,0,0,0",
      "2,0,0,0",
      "3,150,1,0"};
  runTest(dataInput, spineInput, expectedOutput);
}

// Valid spine with some amount of overlap for partner
// No opp_flag flag needed at the output level
TEST_F(LiftIdSpineFileCombinerTest, ValidSpinePartner) {
  FLAGS_multi_conversion_limit = 4;
  std::vector<std::string> dataInput = {
      "id_,event_timestamp,value",
      "123,125,100",
      "111,200,200",
      "222,375,300",
      "333,400,400"};
  std::vector<std::string> spineInput = {
      "1,123", "2,", "10,111", "DDDD,", "EEEE,222", "FFFF,333"};
  // We tread id_ column as a string
  // so the sort will based on lexicographical order.
  std::vector<std::string> expectedOutput = {
      "id_,event_timestamps,values",
      "1,[0,0,0,125],[0,0,0,100]",
      "10,[0,0,0,200],[0,0,0,200]",
      "2,[0,0,0,0],[0,0,0,0]",
      "DDDD,[0,0,0,0],[0,0,0,0]",
      "EEEE,[0,0,0,375],[0,0,0,300]",
      "FFFF,[0,0,0,400],[0,0,0,400]"};
  runTest(dataInput, spineInput, expectedOutput);
}

// Valid spine with some amount of overlap for partner, using hashed ids
// No opp_flag flag needed at the output level
TEST_F(LiftIdSpineFileCombinerTest, ValidSpinePartnerWithHashedId) {
  FLAGS_multi_conversion_limit = 4;
  std::vector<std::string> dataInput = {
      "id_,event_timestamp,value",
      "cfcd208495d565ef66e7dff9f98764da,125,100",
      "c4ca4238a0b923820dcc509a6f75849b,200,200",
      "c81e728d9d4c2f636f067f89cc14862c,375,300",
      "6512bd43d9caa6e02c990b0a82652dca,400,400"};
  std::vector<std::string> spineInput = {
      "AAAA,cfcd208495d565ef66e7dff9f98764da",
      "BBBB,c4ca4238a0b923820dcc509a6f75849b",
      "CCCC,",
      "DDDD,",
      "EEEE,c81e728d9d4c2f636f067f89cc14862c",
      "FFFF,6512bd43d9caa6e02c990b0a82652dca"};
  std::vector<std::string> expectedOutput = {
      "id_,event_timestamps,values",
      "AAAA,[0,0,0,125],[0,0,0,100]",
      "BBBB,[0,0,0,200],[0,0,0,200]",
      "CCCC,[0,0,0,0],[0,0,0,0]",
      "DDDD,[0,0,0,0],[0,0,0,0]",
      "EEEE,[0,0,0,375],[0,0,0,300]",
      "FFFF,[0,0,0,400],[0,0,0,400]"};
  runTest(dataInput, spineInput, expectedOutput);
}

// Valid spine with some amount of overlap for partner
// No opp_flag flag needed at the output level
// Multiple conversion needs to be processed
TEST_F(LiftIdSpineFileCombinerTest, ValidSpinePartnerMultiConversion) {
  FLAGS_multi_conversion_limit = 4;
  std::vector<std::string> dataInput = {
      "id_,event_timestamp,value",
      "123,125,102",
      "123,126,103",
      "123,127,104",
      "123,128,105",
      "123,129,106",
      "111,200,200",
      "222,375,300",
      "333,400,400"};
  std::vector<std::string> spineInput = {
      "AAAA,123", "BBBB,111", "CCCC,", "DDDD,", "EEEE,222", "FFFF,333"};
  std::vector<std::string> expectedOutput = {
      "id_,event_timestamps,values",
      "AAAA,[125,126,127,128],[102,103,104,105]",
      "BBBB,[0,0,0,200],[0,0,0,200]",
      "CCCC,[0,0,0,0],[0,0,0,0]",
      "DDDD,[0,0,0,0],[0,0,0,0]",
      "EEEE,[0,0,0,375],[0,0,0,300]",
      "FFFF,[0,0,0,400],[0,0,0,400]"};
  runTest(dataInput, spineInput, expectedOutput);
}

// Multiple conversion needs to be processed
// Allowed number of conversions set to 2
TEST_F(LiftIdSpineFileCombinerTest, ValidSpinePartnerMultiConversionLimited) {
  FLAGS_multi_conversion_limit = 2;
  std::vector<std::string> dataInput = {
      "id_,event_timestamp,value",
      "123,125,102",
      "123,126,103",
      "123,127,104",
      "123,128,105",
      "123,129,106",
      "111,200,200",
      "222,375,300",
      "333,400,400"};
  std::vector<std::string> spineInput = {
      "AAAA,123", "BBBB,111", "CCCC,", "DDDD,", "EEEE,222", "FFFF,333"};
  std::vector<std::string> expectedOutput = {
      "id_,event_timestamps,values",
      "AAAA,[125,126],[102,103]",
      "BBBB,[0,200],[0,200]",
      "CCCC,[0,0],[0,0]",
      "DDDD,[0,0],[0,0]",
      "EEEE,[0,375],[0,300]",
      "FFFF,[0,400],[0,400]"};
  runTest(dataInput, spineInput, expectedOutput);
}

// Valid input for a valueless nonsales objective
TEST_F(LiftIdSpineFileCombinerTest, ValidSpinePartnerNoValueColumn) {
  FLAGS_multi_conversion_limit = 4;

  std::vector<std::string> dataInput = {
      "id_,event_timestamp",
      "123,125",
      "123,126",
      "123,127",
      "123,128",
      "123,129",
      "111,200",
      "222,375",
      "333,400"};
  std::vector<std::string> spineInput = {
      "AAAA,123", "BBBB,111", "CCCC,", "DDDD,", "EEEE,222", "FFFF,333"};
  std::vector<std::string> expectedOutput = {
      "id_,event_timestamps",
      "AAAA,[125,126,127,128]",
      "BBBB,[0,0,0,200]",
      "CCCC,[0,0,0,0]",
      "DDDD,[0,0,0,0]",
      "EEEE,[0,0,0,375]",
      "FFFF,[0,0,0,400]"};
  runTest(dataInput, spineInput, expectedOutput);
}

// Verify that LiftIdSpineMultiConversionInput sorts values pairwise by time
// and stores them in a sorted manner
TEST_F(LiftIdSpineFileCombinerTest, VerifySortByTime) {
  std::vector<std::string> dataInput = {
      "id_,event_timestamp,value",
      "123,128,105",
      "123,127,104",
      "123,126,103",
      "123,125,102",
  };
  std::vector<std::string> spineInput = {"AAAA,123"};
  std::vector<std::string> expectedOutput = {
      "id_,event_timestamps,values",
      "AAAA,[125,126,127,128],[102,103,104,105]",
  };
  FLAGS_multi_conversion_limit = 4;
  runTest(dataInput, spineInput, expectedOutput);
}
