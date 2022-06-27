/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <cstdint>
#include <exception>
#include <filesystem>
#include <memory>
#include <stdexcept>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include <glog/logging.h>
#include <gtest/gtest.h>

#include <fbpcf/io/FileManagerUtil.h>
#include <sys/types.h>
#include "fbpcs/emp_games/common/TestUtil.h"
#include "fbpcs/emp_games/lift/pcf2_calculator/test/common/LiftCalculator.h"

namespace private_lift {

class LiftCalculatorLocalTestEnvironment : public ::testing::Environment {
 public:
  LiftCalculatorLocalTestEnvironment() {}
  void SetUp() override {
    google::InstallFailureFunction(
        &LiftCalculatorLocalTestEnvironment::HandleFailureFunction);
  }

  static void HandleFailureFunction() {
    LOG(WARNING) << "Error Occured." << std::endl;
    throw std::runtime_error("parse error");
  }

  void TearDown() override {
    LOG(INFO) << "Exiting Test Env" << std::endl;
  }
};

class LiftCalculatorLocalTestFixture : public ::testing::Test {};

TEST_F(LiftCalculatorLocalTestFixture, mapColToIndexTest) {
  std::vector<std::string> pubHeader{
      "id_",
      "opportunity",
      "test_flag",
      "num_clicks",
      "num_impressions",
      "total_spend",
      "breakdown_id"};
  std::vector<std::string> partnerHeader{"id_", "event_timestamps", "values"};

  // Declare expected results.
  size_t expectedCols = 9;
  LiftCalculator lc(0, 0, 0);

  auto test_map = lc.mapColToIndex(pubHeader, partnerHeader);
  // checks if the num cols pub+partner - 1 match the size of the map (id_ is
  // common)
  EXPECT_EQ(test_map.size(), expectedCols);
  // checks index of opportunity
  EXPECT_EQ(test_map.at("opportunity"), 1);
  // checks index of event_timestamp
  EXPECT_EQ(test_map.at("event_timestamps"), 1);
  // checks index of id_
  EXPECT_EQ(test_map.at("id_"), 0); // overrides the id_ of the publisher.
  // should throw exception if no column found.
  EXPECT_THROW(test_map.at("num_touch"), std::exception);
}

TEST_F(LiftCalculatorLocalTestFixture, parseTest) {
  std::vector<std::string> testArrays{
      "[123, 0, w123]", "[-123,      567, 000]"};

  LiftCalculator lc(0, 0, 0);

  EXPECT_THROW(lc.parseArray<uint64_t>(testArrays[0]), std::runtime_error);

  auto test_obj = lc.parseArray<int64_t>(testArrays[1]);
  EXPECT_EQ(test_obj.size(), 3);
  EXPECT_EQ(test_obj.at(0), -123);
  EXPECT_EQ(test_obj.at(2), 0);

  std::unordered_map<std::string, int32_t> colNameIndex{
      {"id_", 0}, {"opportunity", 1}, {"num_clicks", 2}, {"offset", 3}};

  std::vector<std::string> test_vec{"0", "123456", "ThisShouldFail", "-12"};

  EXPECT_EQ(
      std::get<0>(lc.parseUint64OrDie("id_", test_vec, colNameIndex)), 0U);

  // check if the named parser for opportunity reads the correct value.
  EXPECT_EQ(
      std::get<0>(lc.parseUint64OrDie("opportunity", test_vec, colNameIndex)),
      123456U);
  // since offset in test_vec is -12, parser should yield its 2s complement
  // of the number represented as uint64.
  EXPECT_NE(
      std::get<0>(lc.parseUint64OrDie("offset", test_vec, colNameIndex)), 12);

  // if the column does not exist raise a runtime_error.
  EXPECT_THROW(
      std::get<0>(lc.parseUint64OrDie("num_clicks", test_vec, colNameIndex)),
      std::runtime_error);
}

TEST_F(LiftCalculatorLocalTestFixture, PrivateMethods) {
  LiftCalculator lc(0, 1, 0);
  GroupedLiftMetrics glm(0, 1);
  glm.reset();
  // Test Check Update test match count
  uint64_t opportunity = 10U;
  uint64_t eventTimestamp = 10U;
  int32_t tsOffset = 10;
  uint8_t cohortId = 0, breakdownId = 0;
  // if eventTimestamp + offset > opportunity has to return true.
  EXPECT_EQ(
      lc.checkAndUpdateControlConversions(
          glm,
          opportunity,
          eventTimestamp,
          tsOffset,
          false,
          cohortId,
          breakdownId),
      true);
  // Check if controlConversions are updated and also controlConverters
  EXPECT_EQ(glm.metrics.controlConversions, 1);
  EXPECT_EQ(glm.metrics.controlConverters, 1);
  EXPECT_EQ(glm.publisherBreakdowns[breakdownId].controlConversions, 1);
  EXPECT_EQ(glm.publisherBreakdowns[breakdownId].controlConverters, 1);

  // if eventTimestamp + offset > opportunity has to return true.
  EXPECT_EQ(
      lc.checkAndUpdateTestConversions(
          glm,
          opportunity,
          eventTimestamp,
          tsOffset,
          true,
          cohortId,
          breakdownId),
      true);
  // Check if testConversions are updated and not testConverters
  EXPECT_EQ(glm.metrics.testConversions, 1);
  EXPECT_EQ(glm.metrics.testConverters, 0);
  EXPECT_EQ(glm.publisherBreakdowns[breakdownId].testConversions, 1);
  EXPECT_EQ(glm.publisherBreakdowns[breakdownId].testConverters, 0);

  tsOffset = 0;
  // tsOffset is 0 so eventTimestamp == opportunity, should return false.
  EXPECT_EQ(
      lc.checkAndUpdateControlConversions(
          glm,
          opportunity,
          eventTimestamp,
          tsOffset,
          false,
          cohortId,
          breakdownId),
      false);

  // testMatchCount should increment for this test input.
  // both breakdown metrics and metrics should be updated.
  EXPECT_EQ(
      lc.checkAndUpdateTestMatchCount(
          glm, opportunity, eventTimestamp, false, cohortId, breakdownId),
      true);
  EXPECT_EQ(glm.metrics.testMatchCount, 1);
  EXPECT_EQ(glm.publisherBreakdowns[breakdownId].testMatchCount, 1);

  // if the breakdownId is out of range any update should throw
  // out_of_range exception.
  breakdownId = 1;
  EXPECT_THROW(
      lc.checkAndUpdateTestMatchCount(
          glm, opportunity, eventTimestamp, false, cohortId, breakdownId),
      std::out_of_range);

  breakdownId = 0;
  // countMatchedAlready is true, this should not update controlMatchCounts.
  EXPECT_EQ(
      lc.checkAndUpdateControlMatchCount(
          glm, opportunity, eventTimestamp, true, cohortId, breakdownId),
      false);
  EXPECT_EQ(glm.metrics.controlMatchCount, 0);
  EXPECT_EQ(glm.publisherBreakdowns[breakdownId].controlMatchCount, 0);
}

GroupedLiftMetrics getLiftMetrics() {
  uint64_t epoch = 1546300800;
  uint64_t numCohorts = 3;
  uint64_t numPublisherBreakdowns = 2;
  std::string baseDir =
      private_measurement::test_util::getBaseDirFromPath(__FILE__);
  std::string publisherInputPath =
      baseDir + "../../../sample_input/publisher_unittest3.csv";
  std::string partnerInputPath =
      baseDir + "../../../sample_input/partner_2_convs_unittest.csv";

  LiftCalculator liftCalculator(numCohorts, numPublisherBreakdowns, epoch);
  std::ifstream inFilePublisher{publisherInputPath};
  std::ifstream inFilePartner{partnerInputPath};
  int32_t tsOffset = 10;
  std::string linePublisher;
  std::string linePartner;
  getline(inFilePublisher, linePublisher);
  getline(inFilePartner, linePartner);
  auto headerPublisher =
      private_measurement::csv::splitByComma(linePublisher, false);
  auto headerPartner =
      private_measurement::csv::splitByComma(linePartner, false);
  std::unordered_map<std::string, int> colNameToIndex =
      liftCalculator.mapColToIndex(headerPublisher, headerPartner);
  GroupedLiftMetrics result = liftCalculator.compute(
      inFilePublisher, inFilePartner, colNameToIndex, tsOffset, false);

  inFilePartner.close();
  inFilePublisher.close();

  return result;
}

TEST(LiftCalculator, FormatTest) {
  auto result = getLiftMetrics();

  // Checks the dimension of the groupedLiftMetrics returned.
  EXPECT_EQ(result.publisherBreakdowns.size(), 2);
  EXPECT_EQ(result.cohortMetrics.size(), 3);
}

TEST(LiftCalculatorLocalTest, JsonCorrectnessTest) {
  std::string baseDir =
      private_measurement::test_util::getBaseDirFromPath(__FILE__);
  std::string expectedOutputPath =
      baseDir + "../../../sample_input/correctness_output.json";
  GroupedLiftMetrics expectedResult =
      GroupedLiftMetrics::fromJson(fbpcf::io::read(expectedOutputPath));

  auto result = getLiftMetrics();
  EXPECT_EQ(result, expectedResult);
}

} // namespace private_lift

int main(int argc, char* argv[]) {
  ::testing::InitGoogleTest(&argc, argv);
  ::google::InitGoogleLogging(argv[0]);
  ::testing::AddGlobalTestEnvironment(
      new private_lift::LiftCalculatorLocalTestEnvironment);
  return RUN_ALL_TESTS();
}
