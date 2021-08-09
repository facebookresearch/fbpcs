/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <filesystem>
#include <memory>
#include <string>
#include <thread>
#include <unordered_map>

#include <emp-sh2pc/emp-sh2pc.h>
#include <gtest/gtest.h>

#include "folly/Random.h"

#include <fbpcf/mpc/EmpGame.h>
#include <fbpcf/mpc/EmpTestUtil.h>
#include "../../../common/Csv.h"
#include "../../common/GroupedLiftMetrics.h"
#include "../CalculatorGame.h"
#include "../CalculatorGameConfig.h"
#include "../InputData.h"
#include "../OutputMetrics.h"
#include "common/GenFakeData.h"
#include "common/LiftCalculator.h"

namespace private_lift {
class CalculatorGameTest : public ::testing::Test {
 public:
  CalculatorGameConfig getInputData(
      const std::filesystem::path& inputPath,
      bool isConversionLift) {
    int32_t numConversionsPerUser = isConversionLift ? 4 : 1;
    int64_t epoch = 1546300800;

    auto liftGranularityType = isConversionLift
        ? InputData::LiftGranularityType::Conversion
        : InputData::LiftGranularityType::Converter;

    InputData inputData{
        inputPath,
        InputData::LiftMPCType::Standard,
        liftGranularityType,
        epoch,
        numConversionsPerUser};
    CalculatorGameConfig config = {
        inputData, isConversionLift, numConversionsPerUser};
    return config;
  }

 protected:
  std::string aliceInputFilename_;
  std::string bobInputFilename_;

  void SetUp() override {
    std::string tempDir = std::filesystem::temp_directory_path();
    aliceInputFilename_ = folly::sformat(
        "{}/publisher_{}.csv", tempDir, folly::Random::secureRand64());
    bobInputFilename_ = folly::sformat(
        "{}/partner_{}.csv", tempDir, folly::Random::secureRand64());
  }

  void TearDown() override {
    std::filesystem::remove(aliceInputFilename_);
    std::filesystem::remove(bobInputFilename_);
  }

  void runTest(
      CalculatorGameConfig aliceConfig,
      CalculatorGameConfig bobConfig) {
    // compute results with CalculatorGame
    auto res = fbpcf::mpc::
        test<CalculatorGame<fbpcf::QueueIO>, CalculatorGameConfig, std::string>(
            aliceConfig, bobConfig);
    GroupedLiftMetrics resFirst = GroupedLiftMetrics::fromJson(res.first);
    GroupedLiftMetrics resSecond = GroupedLiftMetrics::fromJson(res.second);

    // calculate expected results with simple lift calculator
    LiftCalculator liftCalculator;
    std::ifstream inFileAlice{aliceInputFilename_};
    std::ifstream inFileBob{bobInputFilename_};
    int32_t tsOffset = 10;
    std::string linePublisher;
    std::string linePartner;
    getline(inFileAlice, linePublisher);
    getline(inFileBob, linePartner);
    auto headerPublisher =
        private_measurement::csv::splitByComma(linePublisher, false);
    auto headerPartner =
        private_measurement::csv::splitByComma(linePartner, false);
    std::unordered_map<std::string, int> colNameToIndex =
        liftCalculator.mapColToIndex(headerPublisher, headerPartner);
    OutputMetricsData computedResult = liftCalculator.compute(
        inFileAlice, inFileBob, colNameToIndex, tsOffset);
    GroupedLiftMetrics expectedRes;
    expectedRes.metrics = computedResult.toLiftMetrics();

    // assert expected results and CalculatorGame calculated results
    EXPECT_EQ(expectedRes, resFirst);
    EXPECT_EQ(expectedRes, resSecond);
  }
};

TEST_F(CalculatorGameTest, TestRandomInputConversionLift) {
  // generate test input files with random data
  GenFakeData testDataGenerator;
  LiftFakeDataParams params;
  params.setNumRows(15)
      .setOpportunityRate(0.5)
      .setTestRate(0.5)
      .setPurchaseRate(0.5)
      .setIncrementalityRate(0.0)
      .setEpoch(1546300800);
  testDataGenerator.genFakePublisherInputFile(aliceInputFilename_, params);
  params.setNumConversions(4).setOmitValuesColumn(false);
  testDataGenerator.genFakePartnerInputFile(bobInputFilename_, params);

  CalculatorGameConfig configRandomConversionAlice =
      CalculatorGameTest::getInputData(aliceInputFilename_, true);
  CalculatorGameConfig configRandomConversionBob =
      CalculatorGameTest::getInputData(bobInputFilename_, true);

  runTest(
      std::move(configRandomConversionAlice),
      std::move(configRandomConversionBob));
}

TEST_F(CalculatorGameTest, TestRandomInputConversionLiftValueless) {
  // generate test input files with random data
  GenFakeData testDataGenerator;
  LiftFakeDataParams params;
  params.setNumRows(15)
      .setOpportunityRate(0.5)
      .setTestRate(0.5)
      .setPurchaseRate(0.5)
      .setIncrementalityRate(0.0)
      .setEpoch(1546300800);
  testDataGenerator.genFakePublisherInputFile(aliceInputFilename_, params);
  params.setNumConversions(4).setOmitValuesColumn(true);
  testDataGenerator.genFakePartnerInputFile(bobInputFilename_, params);

  CalculatorGameConfig configRandomConversionAlice =
      CalculatorGameTest::getInputData(aliceInputFilename_, true);
  CalculatorGameConfig configRandomConversionBob =
      CalculatorGameTest::getInputData(bobInputFilename_, true);

  runTest(
      std::move(configRandomConversionAlice),
      std::move(configRandomConversionBob));
}
} // namespace private_lift
