/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "fbpcs/data_processing/attribution_id_combiner/PidAttributionIdCombiner.h"

#include <folly/Random.h>
#include <gflags/gflags.h>
#include <gtest/gtest.h>
#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <fstream>

#include "fbpcf/io/api/FileIOWrappers.h"
#include "fbpcs/data_processing/attribution_id_combiner/AttributionIdSpineCombinerOptions.h"
#include "fbpcs/data_processing/test_utils/FileIOTestUtils.h"

using namespace ::pid::combiner;

class PidAttributionIdCombinerTest : public testing::Test {
 public:
  void createData(
      std::vector<std::string>& dataContent,
      std::vector<std::string>& spineIdContent) {
    auto randStart = folly::Random::secureRand64();
    std::string dataContentPath =
        "/tmp/AttributionIdSpineFileCombinerTestDataContent" +
        std::to_string(randStart);
    std::string spineIdContentPath =
        "/tmp/AttributionIdSpineFileCombinerTestSpineIdContent" +
        std::to_string(randStart);
    data_processing::test_utils::writeVecToFile(dataContent, dataContentPath);
    data_processing::test_utils::writeVecToFile(
        spineIdContent, spineIdContentPath);
    FLAGS_output_path = "/tmp/AttributionIdSpineFileCombinerTestOutputContent" +
        std::to_string(randStart);
    FLAGS_data_path = dataContentPath;
    FLAGS_spine_path = spineIdContentPath;
  }

  void createPublisherData() {
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
    createData(dataInput, spineInput);
  }

  void createPartnerData() {
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
};

TEST_F(PidAttributionIdCombinerTest, TestVerfiyHeaderPlublsiher) {
  createPublisherData();
  std::string header = "id_,ad_id,timestamp,is_click,campaign_metadata";
  std::stringstream outputStream_;
  PidAttributionIdCombiner p;

  bool res = p.getFileType(header);

  EXPECT_EQ(res, true);
}

TEST_F(PidAttributionIdCombinerTest, TestVerfiyHeaderPartner) {
  createPartnerData();
  std::string header =
      "id_,conversion_timestamp,conversion_value,conversion_metadata";
  std::stringstream outputStream_;
  PidAttributionIdCombiner p;

  bool res = p.getFileType(header);

  EXPECT_EQ(res, false);
}

TEST_F(PidAttributionIdCombinerTest, TestIncorrectHeader) {
  createPartnerData();
  std::string header = "campaign_metadata";
  PidAttributionIdCombiner p;

  ASSERT_DEATH(p.getFileType(header), ".*Invalid headers for dataset.*");
}

TEST_F(PidAttributionIdCombinerTest, TestProcessPublisher) {
  createPublisherData();
  PidAttributionIdCombiner p;

  auto dataReader = std::make_unique<fbpcf::io::FileReader>(FLAGS_data_path);
  auto dataFile = std::make_shared<fbpcf::io::BufferedReader>(
      std::move(dataReader), fbpcf::io::kBufferedReaderChunkSize);
  FileMetaData res = p.processHeader(dataFile);

  std::vector<std::string> col{
      "ad_id", "timestamp", "is_click", "campaign_metadata"};
  EXPECT_EQ(res.headerLine, "id_,ad_id,timestamp,is_click,campaign_metadata");
  EXPECT_EQ(res.isPublisherDataset, true);
  EXPECT_EQ(res.aggregatedCols, col);
}

TEST_F(PidAttributionIdCombinerTest, TestProcessPartner) {
  createPartnerData();
  PidAttributionIdCombiner p;

  auto dataReader = std::make_unique<fbpcf::io::FileReader>(FLAGS_data_path);
  auto dataFile = std::make_shared<fbpcf::io::BufferedReader>(
      std::move(dataReader), fbpcf::io::kBufferedReaderChunkSize);
  FileMetaData res = p.processHeader(dataFile);

  std::vector<std::string> col{
      "conversion_timestamp", "conversion_value", "conversion_metadata"};
  EXPECT_EQ(
      res.headerLine,
      "id_,conversion_timestamp,conversion_value,conversion_metadata");
  EXPECT_EQ(res.isPublisherDataset, false);
  EXPECT_EQ(res.aggregatedCols, col);
}

TEST_F(PidAttributionIdCombinerTest, TestRun) {
  createPartnerData();
  PidAttributionIdCombiner p;

  p.run();

  auto outputFile = readfile();
  std::string expectedStr =
      "id_,conversion_timestamps,conversion_values,conversion_metadata\nAAAA,[0,0,1656361100,1656361200],[0,0,100,50],[0,0,1,2]\nBBBB,[0,0,0,1656361200],[0,0,0,10],[0,0,0,3]\nCCCC,[0,0,0,0],[0,0,0,0],[0,0,0,0]\nDDDD,[0,0,0,0],[0,0,0,0],[0,0,0,0]\nEEEE,[0,0,0,1656361300],[0,0,0,20],[0,0,0,4]\nFFFF,[0,0,1656361400,1656361500],[0,0,0,25],[0,0,5,6]\n";
  EXPECT_EQ(outputFile.str(), expectedStr);
}
