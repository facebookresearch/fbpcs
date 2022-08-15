/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "fbpcs/data_processing/lift_id_combiner/PidLiftIdCombiner.h"

#include <folly/Random.h>
#include <gflags/gflags.h>
#include <gtest/gtest.h>
#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <fstream>

#include "fbpcs/data_processing/lift_id_combiner/LiftIdSpineCombinerOptions.h"
#include "fbpcs/data_processing/test_utils/FileIOTestUtils.h"

using namespace ::pid::combiner;
using namespace std::chrono;
class PidLiftIdCombinerTest : public testing::Test {
 public:
  void TearDown() override {
    // Call base TearDown
    Test::TearDown();
    // All done, cleanup files now
    std::remove(dataFilePath_.c_str());
    std::remove(spineFilePath_.c_str());
    std::remove(outputFilePath_.c_str());
  }
  void createData(
      std::vector<std::string>& dataContent,
      std::vector<std::string>& spineIdContent) {
    // Generate unique filenames for each run to avoid  race condition
    int64_t timestamp =
        duration_cast<nanoseconds>(system_clock::now().time_since_epoch())
            .count();

    dataFilePath_ =
        "/tmp/LiftTestDataContent" + std::to_string(timestamp) + "data.txt";
    spineFilePath_ =
        "/tmp/LiftTestDataContent" + std::to_string(timestamp) + "spine.txt";
    outputFilePath_ =
        "/tmp/LiftTestDataContent" + std::to_string(timestamp) + "output.txt";
    data_processing::test_utils::writeVecToFile(dataContent, dataFilePath_);
    data_processing::test_utils::writeVecToFile(spineIdContent, spineFilePath_);
    FLAGS_output_path = outputFilePath_;
    FLAGS_data_path = dataFilePath_;
    FLAGS_spine_path = spineFilePath_;
  }

  void createPublisherData() {
    std::vector<std::string> dataInput = {
        "id_,opportunity_timestamp,test_flag",
        "aaa,100,1",
        "bbb,150,0",
        "ccc,200,0"};
    std::vector<std::string> spineInput = {
        "1,aaa", "2,", "3,bbb", "10,ccc", "100,", "123,"};
    createData(dataInput, spineInput);
  }

  void createPartnerData() {
    std::vector<std::string> dataInput = {
        "id_,event_timestamp,value",
        "123,125,100",
        "111,200,200",
        "222,375,300",
        "333,400,400"};
    std::vector<std::string> spineInput = {
        "1,123", "2,", "10,111", "DDDD,", "EEEE,222", "FFFF,333"};
    createData(dataInput, spineInput);
  }

  std::stringstream readfile() {
    std::ifstream outputFile{FLAGS_output_path};
    std::stringstream s;
    if (outputFile) {
      s << outputFile.rdbuf();
      outputFile.close();
    }
    return s;
  }

 protected:
  std::string dataFilePath_;
  std::string spineFilePath_;
  std::string outputFilePath_;
};

TEST_F(PidLiftIdCombinerTest, TestVerfiyHeaderPlublsiher) {
  createPublisherData();
  std::string header = "id_,opportunity_timestamp,test_flag";
  std::stringstream outputStream_;
  PidLiftIdCombiner p(
      FLAGS_data_path,
      FLAGS_spine_path,
      FLAGS_output_path,
      FLAGS_tmp_directory,
      FLAGS_sort_strategy,
      FLAGS_max_id_column_cnt,
      FLAGS_protocol_type);

  bool res = p.getFileType(header);

  EXPECT_EQ(res, true);
}

TEST_F(PidLiftIdCombinerTest, TestVerfiyHeaderPartner) {
  createPartnerData();
  std::string header = "id_,event_timestamp,value";
  std::stringstream outputStream_;
  PidLiftIdCombiner p(
      FLAGS_data_path,
      FLAGS_spine_path,
      FLAGS_output_path,
      FLAGS_tmp_directory,
      FLAGS_sort_strategy,
      FLAGS_max_id_column_cnt,
      FLAGS_protocol_type);

  bool res = p.getFileType(header);

  EXPECT_EQ(res, false);
}

TEST_F(PidLiftIdCombinerTest, TestIncorrectHeader) {
  createPartnerData();
  std::string header = "campaign_metadata";
  PidLiftIdCombiner p(
      FLAGS_data_path,
      FLAGS_spine_path,
      FLAGS_output_path,
      FLAGS_tmp_directory,
      FLAGS_sort_strategy,
      FLAGS_max_id_column_cnt,
      FLAGS_protocol_type);

  ASSERT_DEATH(p.getFileType(header), ".*Invalid headers for dataset.*");
}

TEST_F(PidLiftIdCombinerTest, TestProcessPublisher) {
  createPublisherData();
  PidLiftIdCombiner p(
      FLAGS_data_path,
      FLAGS_spine_path,
      FLAGS_output_path,
      FLAGS_tmp_directory,
      FLAGS_sort_strategy,
      FLAGS_max_id_column_cnt,
      FLAGS_protocol_type);

  auto dataReader = std::make_unique<fbpcf::io::FileReader>(FLAGS_data_path);
  auto dataFile = std::make_shared<fbpcf::io::BufferedReader>(
      std::move(dataReader), fbpcf::io::kBufferedReaderChunkSize);
  FileMetaData res = p.processHeader(dataFile);

  EXPECT_EQ(res.headerLine, "id_,opportunity_timestamp,test_flag");
  EXPECT_EQ(res.isPublisherDataset, true);
}

TEST_F(PidLiftIdCombinerTest, TestProcessPartner) {
  createPartnerData();
  PidLiftIdCombiner p(
      FLAGS_data_path,
      FLAGS_spine_path,
      FLAGS_output_path,
      FLAGS_tmp_directory,
      FLAGS_sort_strategy,
      FLAGS_max_id_column_cnt,
      FLAGS_protocol_type);

  auto dataReader = std::make_unique<fbpcf::io::FileReader>(FLAGS_data_path);
  auto dataFile = std::make_shared<fbpcf::io::BufferedReader>(
      std::move(dataReader), fbpcf::io::kBufferedReaderChunkSize);
  FileMetaData res = p.processHeader(dataFile);

  EXPECT_EQ(res.headerLine, "id_,event_timestamp,value");
  EXPECT_EQ(res.isPublisherDataset, false);
}

TEST_F(PidLiftIdCombinerTest, TestRun) {
  createPartnerData();
  PidLiftIdCombiner p(
      FLAGS_data_path,
      FLAGS_spine_path,
      FLAGS_output_path,
      FLAGS_tmp_directory,
      FLAGS_sort_strategy,
      FLAGS_max_id_column_cnt,
      FLAGS_protocol_type);

  p.run();

  auto outputFile = readfile();
  std::string expectedStr =
      "id_,event_timestamps,values\n1,[0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,125],[0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,100]\n10,[0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,200],[0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,200]\n2,[0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0],[0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0]\nDDDD,[0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0],[0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0]\nEEEE,[0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,375],[0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,300]\nFFFF,[0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,400],[0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,400]\n";
  EXPECT_EQ(outputFile.str(), expectedStr);
}
