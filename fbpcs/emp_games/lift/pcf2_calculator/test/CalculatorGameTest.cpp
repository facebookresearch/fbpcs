/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <filesystem>
#include <memory>
#include <string>
#include <thread>
#include <unordered_map>

#include <fbpcs/emp_games/lift/common/GroupedLiftMetrics.h>
#include <gtest/gtest.h>
#include "folly/Random.h"

#include <fbpcf/io/api/FileIOWrappers.h>
#include "fbpcf/engine/communication/InMemoryPartyCommunicationAgentFactory.h"
#include "fbpcf/engine/communication/test/AgentFactoryCreationHelper.h"
#include "fbpcf/test/TestHelper.h"
#include "fbpcs/emp_games/common/Csv.h"
#include "fbpcs/emp_games/common/TestUtil.h"
#include "fbpcs/emp_games/common/test/TestUtils.h"
#include "fbpcs/emp_games/lift/pcf2_calculator/CalculatorGame.h"
#include "fbpcs/emp_games/lift/pcf2_calculator/test/common/GenFakeData.h"
#include "fbpcs/emp_games/lift/pcf2_calculator/test/common/LiftCalculator.h"

namespace private_lift {

template <int schedulerId>
GroupedLiftMetrics runCalculatorGame(
    int myId,
    CalculatorGameConfig config,
    std::shared_ptr<
        fbpcf::engine::communication::IPartyCommunicationAgentFactory> factory,
    fbpcf::SchedulerCreator schedulerCreator) {
  auto scheduler = schedulerCreator(myId, *factory);
  auto game = std::make_unique<CalculatorGame<schedulerId>>(
      myId, std::move(scheduler), std::move(factory));
  auto output = game->play(config);
  return GroupedLiftMetrics::fromJson(output);
}

class CalculatorGameTestFixture
    : public ::testing::TestWithParam<std::tuple<common::SchedulerType, bool>> {
 public:
  CalculatorGameConfig getInputData(
      const std::filesystem::path& inputPath,
      int numConversionsPerUser,
      bool computePublisherBreakdowns) {
    int64_t epoch = 1546300800;
    InputData inputData{
        inputPath,
        InputData::LiftMPCType::Standard,
        computePublisherBreakdowns,
        epoch,
        numConversionsPerUser};
    CalculatorGameConfig config = {inputData, true, numConversionsPerUser};
    return config;
  }

 protected:
  std::string publisherInputFilename_;
  std::string partnerInputFilename_;

  void SetUp() override {
    std::string tempDir = std::filesystem::temp_directory_path();
    publisherInputFilename_ = folly::sformat(
        "{}/publisher_{}.csv", tempDir, folly::Random::secureRand64());
    partnerInputFilename_ = folly::sformat(
        "{}/partner_{}.csv", tempDir, folly::Random::secureRand64());
  }

  void TearDown() override {
    std::filesystem::remove(publisherInputFilename_);
    std::filesystem::remove(partnerInputFilename_);
  }

  GroupedLiftMetrics runGameWithScheduler(
      fbpcf::SchedulerCreator schedulerCreator,
      CalculatorGameConfig publisherConfig,
      CalculatorGameConfig partnerConfig) {
    auto factories = fbpcf::engine::communication::getInMemoryAgentFactory(2);
    auto future0 = std::async(
        runCalculatorGame<0>,
        0,
        publisherConfig,
        std::move(factories[0]),
        schedulerCreator);
    auto future1 = std::async(
        runCalculatorGame<1>,
        1,
        partnerConfig,
        std::move(factories[1]),
        schedulerCreator);

    GroupedLiftMetrics resFirst = future0.get();
    GroupedLiftMetrics resSecond = future1.get();

    return resFirst ^ resSecond;
  }

  GroupedLiftMetrics runTestWithCohortAndBreakdown(
      int numCohort,
      int numBreakdown) {
    // Generate test input files with random data
    int numConversionsPerUser = 25;
    bool computePublisherBreakdowns = std::get<1>(GetParam());

    GenFakeData testDataGenerator;
    LiftFakeDataParams params;
    params.setNumRows(15)
        .setOpportunityRate(0.5)
        .setTestRate(0.5)
        .setPurchaseRate(0.5)
        .setIncrementalityRate(0.0)
        .setEpoch(1546300800)
        .setNumBreakdowns(numBreakdown)
        .setNumCohorts(numCohort);
    testDataGenerator.genFakePublisherInputFile(
        publisherInputFilename_, params);
    params.setNumConversions(numConversionsPerUser).setOmitValuesColumn(false);
    testDataGenerator.genFakePartnerInputFile(partnerInputFilename_, params);
    CalculatorGameConfig publisherConfig =
        CalculatorGameTestFixture::getInputData(
            publisherInputFilename_,
            numConversionsPerUser,
            computePublisherBreakdowns);
    CalculatorGameConfig partnerConfig =
        CalculatorGameTestFixture::getInputData(
            partnerInputFilename_,
            numConversionsPerUser,
            computePublisherBreakdowns);

    // Run calculator game with test input
    const bool unsafe = true;
    auto schedulerType = std::get<0>(GetParam());
    fbpcf::SchedulerCreator schedulerCreator =
        fbpcf::getSchedulerCreator<unsafe>(schedulerType);
    auto res = runGameWithScheduler(
        schedulerCreator, std::move(publisherConfig), std::move(partnerConfig));

    return res;
  }

  GroupedLiftMetrics computeExpectedResult(int numCohort, int numBreakdown) {
    LiftCalculator liftCalculator(numCohort, numBreakdown, 0);
    std::ifstream inFilePublisher{publisherInputFilename_};
    std::ifstream inFilePartner{partnerInputFilename_};
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
    GroupedLiftMetrics expectedResult = liftCalculator.compute(
        inFilePublisher, inFilePartner, colNameToIndex, tsOffset, false);

    return expectedResult;
  }
};

TEST_P(CalculatorGameTestFixture, TestCorrectness) {
  int numConversionsPerUser = 2;

  // test with and w/o computing publisher breakdowns
  bool computePublisherBreakdowns = std::get<1>(GetParam());
  std::string baseDir =
      private_measurement::test_util::getBaseDirFromPath(__FILE__);
  CalculatorGameConfig publisherConfig =
      CalculatorGameTestFixture::getInputData(
          baseDir + "../sample_input/publisher_unittest3.csv",
          numConversionsPerUser,
          computePublisherBreakdowns);
  CalculatorGameConfig partnerConfig = CalculatorGameTestFixture::getInputData(
      baseDir + "../sample_input/partner_2_convs_unittest.csv",
      numConversionsPerUser,
      computePublisherBreakdowns);
  std::string expectedOutputFilename =
      baseDir + "../sample_input/correctness_output.json";

  // Run calculator game with input files
  const bool unsafe = true;

  auto schedulerType = std::get<0>(GetParam());
  fbpcf::SchedulerCreator schedulerCreator =
      fbpcf::getSchedulerCreator<unsafe>(schedulerType);
  auto res = runGameWithScheduler(
      schedulerCreator, std::move(publisherConfig), std::move(partnerConfig));

  // Read expected output from file
  auto expectedRes = GroupedLiftMetrics::fromJson(
      fbpcf::io::FileIOWrappers::readFile(expectedOutputFilename));

  // No publisher breakdown computation required, remove the
  // breakdown data from the expected output before result validation
  if (!computePublisherBreakdowns) {
    expectedRes.publisherBreakdowns.clear();
  }

  EXPECT_EQ(expectedRes, res);
}

TEST_P(CalculatorGameTestFixture, TestCorrectnessRandomInput) {
  // Generate test input files with random data
  int numConversionsPerUser = 25;
  bool computePublisherBreakdowns = std::get<1>(GetParam());

  GenFakeData testDataGenerator;
  LiftFakeDataParams params;
  params.setNumRows(15)
      .setOpportunityRate(0.5)
      .setTestRate(0.5)
      .setPurchaseRate(0.5)
      .setIncrementalityRate(0.0)
      .setEpoch(1546300800);
  testDataGenerator.genFakePublisherInputFile(publisherInputFilename_, params);
  params.setNumConversions(numConversionsPerUser).setOmitValuesColumn(false);
  testDataGenerator.genFakePartnerInputFile(partnerInputFilename_, params);
  CalculatorGameConfig publisherConfig =
      CalculatorGameTestFixture::getInputData(
          publisherInputFilename_,
          numConversionsPerUser,
          computePublisherBreakdowns);
  CalculatorGameConfig partnerConfig = CalculatorGameTestFixture::getInputData(
      partnerInputFilename_, numConversionsPerUser, computePublisherBreakdowns);

  // Run calculator game with test input
  const bool unsafe = true;
  auto schedulerType = std::get<0>(GetParam());
  fbpcf::SchedulerCreator schedulerCreator =
      fbpcf::getSchedulerCreator<unsafe>(schedulerType);
  auto res = runGameWithScheduler(
      schedulerCreator, std::move(publisherConfig), std::move(partnerConfig));

  // Calculate expected results with simple lift calculator
  LiftCalculator liftCalculator(0, 0, 0);
  std::ifstream inFilePublisher{publisherInputFilename_};
  std::ifstream inFilePartner{partnerInputFilename_};
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
  GroupedLiftMetrics expectedResult = liftCalculator.compute(
      inFilePublisher, inFilePartner, colNameToIndex, tsOffset, false);
  EXPECT_EQ(expectedResult, res);
}

TEST_P(CalculatorGameTestFixture, TestCorrectnessWithBreakdown) {
  int numCohort = 0;
  int numBreakdown = 2;
  bool computePublisherBreakdowns = std::get<1>(GetParam());

  GroupedLiftMetrics res =
      runTestWithCohortAndBreakdown(numCohort, numBreakdown);

  // Calculate expected results with simple lift calculator
  GroupedLiftMetrics expectedResult =
      computeExpectedResult(numCohort, numBreakdown);

  // No publisher breakdown computation required, remove the
  // breakdown data from the expected output before result validation
  if (!computePublisherBreakdowns) {
    expectedResult.publisherBreakdowns.clear();
  }

  EXPECT_EQ(expectedResult, res);
}

TEST_P(CalculatorGameTestFixture, TestCorrectnessWithCohort) {
  int numCohort = 4;
  int numBreakdown = 0;
  bool computePublisherBreakdowns = std::get<1>(GetParam());

  GroupedLiftMetrics res =
      runTestWithCohortAndBreakdown(numCohort, numBreakdown);

  // Calculate expected results with simple lift calculator
  GroupedLiftMetrics expectedResult =
      computeExpectedResult(numCohort, numBreakdown);

  // No publisher breakdown computation required, remove the
  // breakdown data from the expected output before result validation
  if (!computePublisherBreakdowns) {
    expectedResult.publisherBreakdowns.clear();
  }

  EXPECT_EQ(expectedResult, res);
}

TEST_P(CalculatorGameTestFixture, TestCorrectnessWithCohortAndBreakdown) {
  int numCohort = 4;
  int numBreakdown = 2;
  bool computePublisherBreakdowns = std::get<1>(GetParam());

  GroupedLiftMetrics res =
      runTestWithCohortAndBreakdown(numCohort, numBreakdown);

  // Calculate expected results with simple lift calculator
  GroupedLiftMetrics expectedResult =
      computeExpectedResult(numCohort, numBreakdown);

  // No publisher breakdown computation required, remove the
  // breakdown data from the expected output before result validation
  if (!computePublisherBreakdowns) {
    expectedResult.publisherBreakdowns.clear();
  }

  EXPECT_EQ(expectedResult, res);
}

// Test calculator game with different schedulers
INSTANTIATE_TEST_SUITE_P(
    CalculatorGameTest,
    CalculatorGameTestFixture,
    ::testing::Combine(
        ::testing::Values(
            common::SchedulerType::NetworkPlaintext,
            common::SchedulerType::Eager,
            common::SchedulerType::Lazy),
        ::testing::Bool()),
    [](const testing::TestParamInfo<CalculatorGameTestFixture::ParamType>&
           info) {
      auto schedulerType = std::get<0>(info.param);
      std::string computePublisherBreakdowns =
          std::get<1>(info.param) ? "True" : "False";
      return getSchedulerName(schedulerType) + "_ComputePublisherBreakdowns_" +
          computePublisherBreakdowns;
    });

} // namespace private_lift
