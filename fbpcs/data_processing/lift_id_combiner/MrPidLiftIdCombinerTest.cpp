/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "fbpcs/data_processing/lift_id_combiner/MrPidLiftIdCombiner.h"

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
class MrPidLiftIdCombinerTest : public testing::Test {
 public:
  void TearDown() override {
    // Call base TearDown
    Test::TearDown();
    // All done, cleanup files now
    std::remove(spineFilePath_.c_str());
    std::remove(outputFilePath_.c_str());
  }
  void createData(std::vector<std::string>& spineIdContent) {
    // Generate unique filenames for each run to avoid  race condition
    int64_t timestamp =
        duration_cast<nanoseconds>(system_clock::now().time_since_epoch())
            .count();
    spineFilePath_ =
        "/tmp/LiftTestDataContent" + std::to_string(timestamp) + "spine.txt";
    outputFilePath_ =
        "/tmp/LiftTestDataContent" + std::to_string(timestamp) + "output.txt";
    data_processing::test_utils::writeVecToFile(spineIdContent, spineFilePath_);
    FLAGS_output_path = outputFilePath_;
    FLAGS_spine_path = spineFilePath_;
  }

  void createPublisherData() {
    std::vector<std::string> spineInput = {
        "id_,opportunity_timestamp,test_flag",
        "1,100,1",
        "2,0,0",
        "3,150,0",
        "10,200,0",
        "100,0,0",
        "123,0,0"};
    createData(spineInput);
  }

  void createPartnerData() {
    std::vector<std::string> spineInput = {
        "id_,event_timestamp,value",
        "1,125,100",
        "2,0,0",
        "10,200,200",
        "DDDD,0,0",
        "EEEE,375,300",
        "FFFF,400,400"};
    createData(spineInput);
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
  std::string spineFilePath_;
  std::string outputFilePath_;
};

TEST_F(MrPidLiftIdCombinerTest, TestVerfiyHeaderPlublsiher) {
  createPublisherData();
  std::string header = "id_,opportunity_timestamp,test_flag";
  std::stringstream outputStream_;
  MrPidLiftIdCombiner p(
      FLAGS_spine_path,
      FLAGS_output_path,
      FLAGS_tmp_directory,
      FLAGS_sort_strategy,
      FLAGS_max_id_column_cnt,
      FLAGS_protocol_type);

  bool res = p.getFileType(header);

  EXPECT_EQ(res, true);
}

TEST_F(MrPidLiftIdCombinerTest, TestVerfiyHeaderPartner) {
  createPartnerData();
  std::string header = "id_,event_timestamp,value";
  std::stringstream outputStream_;
  MrPidLiftIdCombiner p(
      FLAGS_spine_path,
      FLAGS_output_path,
      FLAGS_tmp_directory,
      FLAGS_sort_strategy,
      FLAGS_max_id_column_cnt,
      FLAGS_protocol_type);

  bool res = p.getFileType(header);

  EXPECT_EQ(res, false);
}

TEST_F(MrPidLiftIdCombinerTest, TestIncorrectHeader) {
  createPartnerData();
  std::string header = "campaign_metadata";
  MrPidLiftIdCombiner p(
      FLAGS_spine_path,
      FLAGS_output_path,
      FLAGS_tmp_directory,
      FLAGS_sort_strategy,
      FLAGS_max_id_column_cnt,
      FLAGS_protocol_type);

  ASSERT_DEATH(p.getFileType(header), ".*Invalid headers for dataset.*");
}

TEST_F(MrPidLiftIdCombinerTest, TestProcessPublisher) {
  createPublisherData();
  MrPidLiftIdCombiner p(
      FLAGS_spine_path,
      FLAGS_output_path,
      FLAGS_tmp_directory,
      FLAGS_sort_strategy,
      FLAGS_max_id_column_cnt,
      FLAGS_protocol_type);

  auto dataReader = std::make_unique<fbpcf::io::FileReader>(FLAGS_spine_path);
  auto dataFile = std::make_shared<fbpcf::io::BufferedReader>(
      std::move(dataReader), fbpcf::io::kBufferedReaderChunkSize);
  FileMetaData res = p.processHeader(dataFile);

  EXPECT_EQ(res.headerLine, "id_,opportunity_timestamp,test_flag");
  EXPECT_EQ(res.isPublisherDataset, true);
}

TEST_F(MrPidLiftIdCombinerTest, TestProcessPartner) {
  createPartnerData();
  MrPidLiftIdCombiner p(
      FLAGS_spine_path,
      FLAGS_output_path,
      FLAGS_tmp_directory,
      FLAGS_sort_strategy,
      FLAGS_max_id_column_cnt,
      FLAGS_protocol_type);

  auto dataReader = std::make_unique<fbpcf::io::FileReader>(FLAGS_spine_path);
  auto dataFile = std::make_shared<fbpcf::io::BufferedReader>(
      std::move(dataReader), fbpcf::io::kBufferedReaderChunkSize);
  FileMetaData res = p.processHeader(dataFile);

  EXPECT_EQ(res.headerLine, "id_,event_timestamp,value");
  EXPECT_EQ(res.isPublisherDataset, false);
}

TEST_F(MrPidLiftIdCombinerTest, TestRun) {
  createPartnerData();
  MrPidLiftIdCombiner p(
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

TEST_F(MrPidLiftIdCombinerTest, TestRunValidSpinePublisherWithDup) {
  FLAGS_sort_strategy = "keep_original";
  std::vector<std::string> spineInput = {
      "id_,opportunity_timestamp,test_flag,num_impressions,num_clicks,total_spend,breakdown_id,unregistered",
      "AAAA,100,1,1,3,200,0,2",
      "AAAA,120,1,2,4,300,1,3",
      "CCCC,150,0,2,2,150,0,4",
      "CCCC,160,0,3,3,250,1,5",
      "DDDD,200,0,2,2,100,0,6",
      "FFFF,0,0,0,0,0,0,0,0",
      "EEEE,0,0,0,0,0,0,0,0",
      "BBBB,0,0,0,0,0,0,0,0",
  };

  createData(spineInput);
  MrPidLiftIdCombiner p(
      FLAGS_spine_path,
      FLAGS_output_path,
      FLAGS_tmp_directory,
      FLAGS_sort_strategy,
      FLAGS_max_id_column_cnt,
      FLAGS_protocol_type);

  p.run();
  auto outputFile = readfile();
  std::string expectedStr =
      "id_,opportunity_timestamp,test_flag,num_impressions,num_clicks,total_spend,breakdown_id,opportunity,unregistered\nBBBB,0,0,0,0,0,0,0,0,0\nEEEE,0,0,0,0,0,0,0,0,0\nFFFF,0,0,0,0,0,0,0,0,0\nDDDD,200,0,2,2,100,0,1,6\nCCCC,150,0,5,5,400,1,1,4\nAAAA,100,1,3,7,500,1,1,2\n";
  EXPECT_EQ(outputFile.str(), expectedStr);
}

TEST_F(MrPidLiftIdCombinerTest, TestRunValidSortedSpinePublisherWithDup) {
  std::vector<std::string> spineInput = {
      "id_,opportunity_timestamp,test_flag,num_impressions,num_clicks,total_spend,breakdown_id,unregistered",
      "1,100,1,1,3,200,0,2",
      "1,120,1,2,4,300,1,3",
      "3,150,0,2,2,150,0,4",
      "3,160,0,3,3,250,1,5",
      "10,200,0,2,2,100,0,6",
      "100,0,0,0,0,0,0,0",
      "123,0,0,0,0,0,0,0",
      "2,0,0,0,0,0,0,0",
  };

  createData(spineInput);
  MrPidLiftIdCombiner p(
      FLAGS_spine_path,
      FLAGS_output_path,
      FLAGS_tmp_directory,
      FLAGS_sort_strategy,
      FLAGS_max_id_column_cnt,
      FLAGS_protocol_type);

  p.run();
  auto outputFile = readfile();
  std::string expectedStr =
      "id_,opportunity_timestamp,test_flag,num_impressions,num_clicks,total_spend,breakdown_id,opportunity,unregistered\n1,100,1,3,7,500,1,1,2\n10,200,0,2,2,100,0,1,6\n100,0,0,0,0,0,0,0,0\n123,0,0,0,0,0,0,0,0\n2,0,0,0,0,0,0,0,0\n3,150,0,5,5,400,1,1,4\n";
  EXPECT_EQ(outputFile.str(), expectedStr);
}
