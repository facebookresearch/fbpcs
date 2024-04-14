/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <gtest/gtest.h>
#include <random>
#include "folly/Format.h"

#include "fbpcf/engine/communication/test/AgentFactoryCreationHelper.h"
#include "fbpcf/test/TestHelper.h"

#include "fbpcs/emp_games/lift/pcf2_calculator/input_processing/GlobalSharingUtils.h"
#include "fbpcs/emp_games/lift/pcf2_calculator/sample_input/SampleInput.h"

namespace private_lift {

void runValidateNumRowsStep(
    LiftGameProcessedData<0>& liftData0,
    LiftGameProcessedData<1>& liftData1) {
  auto future0 = std::async([&liftData0]() {
    return input_processing::validateNumRowsStep<0>(0, liftData0);
  });

  auto future1 = std::async([&liftData1]() {
    return input_processing::validateNumRowsStep<1>(1, liftData1);
  });
  future0.get();
  future1.get();
}

TEST(GlobalSharingUtilsTest, testValidateNumRows) {
  auto communicationAgentFactory =
      fbpcf::engine::communication::getInMemoryAgentFactory(2);
  fbpcf::setupRealBackend<0, 1>(
      *communicationAgentFactory[0], *communicationAgentFactory[1]);

  LiftGameProcessedData<0> liftData0;
  LiftGameProcessedData<1> liftData1;

  liftData0.numRows = 10;
  liftData1.numRows = 10;

  EXPECT_NO_THROW(runValidateNumRowsStep(liftData0, liftData1));

  liftData0.numRows++;

  ASSERT_DEATH(
      runValidateNumRowsStep(liftData0, liftData1),
      "The publisher has 11 rows in their input, while the partner has 10 rows.");
}

void runShareGroupsAndValueBits(
    const InputData& publisherInput,
    const InputData& partnerInput,
    LiftGameProcessedData<0>& publisherOutput,
    LiftGameProcessedData<1>& partnerOutput) {
  auto future0 = std::async([&publisherInput, &publisherOutput]() {
    input_processing::shareNumGroupsStep<0>(0, publisherInput, publisherOutput);
    input_processing::shareBitsForValuesStep<0>(
        0, publisherInput, publisherOutput);
  });

  auto future1 = std::async([&partnerInput, &partnerOutput]() {
    input_processing::shareNumGroupsStep<1>(1, partnerInput, partnerOutput);
    input_processing::shareBitsForValuesStep<1>(1, partnerInput, partnerOutput);
  });

  future0.get();
  future1.get();
}

TEST(GlobalSharingUtilsTest, testGlobalSharingNoBreakdowns) {
  auto communicationAgentFactory =
      fbpcf::engine::communication::getInMemoryAgentFactory(2);
  fbpcf::setupRealBackend<0, 1>(
      *communicationAgentFactory[0], *communicationAgentFactory[1]);
  int epoch = 1546300800;

  LiftGameProcessedData<0> liftData0;
  LiftGameProcessedData<1> liftData1;
  InputData publisherDataNoBreakdowns{
      sample_input::getPublisherInput3().native(),
      InputData::LiftMPCType::Standard,
      false,
      epoch};

  InputData partnerData{
      sample_input::getPartnerInput2().native(),
      InputData::LiftMPCType::Standard,
      false,
      epoch};

  runShareGroupsAndValueBits(
      publisherDataNoBreakdowns, partnerData, liftData0, liftData1);

  EXPECT_EQ(liftData0.numPartnerCohorts, 3);
  EXPECT_EQ(liftData1.numPartnerCohorts, 3);
  EXPECT_EQ(liftData0.numPublisherBreakdowns, 0);
  EXPECT_EQ(liftData1.numPublisherBreakdowns, 0);
  EXPECT_EQ(liftData0.numGroups, 6);
  EXPECT_EQ(liftData1.numGroups, 6);
  EXPECT_EQ(liftData0.numTestGroups, 4);
  EXPECT_EQ(liftData1.numTestGroups, 4);
  EXPECT_EQ(liftData0.valueBits, 10);
  EXPECT_EQ(liftData1.valueBits, 10);
  EXPECT_EQ(liftData0.valueSquaredBits, 15);
  EXPECT_EQ(liftData1.valueSquaredBits, 15);
}

TEST(GlobalSharingUtilsTest, testGlobalSharingWithBreakdowns) {
  auto communicationAgentFactory =
      fbpcf::engine::communication::getInMemoryAgentFactory(2);
  fbpcf::setupRealBackend<0, 1>(
      *communicationAgentFactory[0], *communicationAgentFactory[1]);
  int epoch = 1546300800;

  LiftGameProcessedData<0> liftData0;
  LiftGameProcessedData<1> liftData1;

  InputData publisherDataWithBreakdowns{
      sample_input::getPublisherInput3().native(),
      InputData::LiftMPCType::Standard,
      true,
      epoch};

  InputData partnerData{
      sample_input::getPartnerInput2().native(),
      InputData::LiftMPCType::Standard,
      false,
      epoch};

  runShareGroupsAndValueBits(
      publisherDataWithBreakdowns, partnerData, liftData0, liftData1);

  EXPECT_EQ(liftData0.numPartnerCohorts, 3);
  EXPECT_EQ(liftData1.numPartnerCohorts, 3);
  EXPECT_EQ(liftData0.numPublisherBreakdowns, 2);
  EXPECT_EQ(liftData1.numPublisherBreakdowns, 2);
  EXPECT_EQ(liftData0.numGroups, 12);
  EXPECT_EQ(liftData1.numGroups, 12);
  EXPECT_EQ(liftData0.numTestGroups, 7);
  EXPECT_EQ(liftData1.numTestGroups, 7);
  EXPECT_EQ(liftData0.valueBits, 10);
  EXPECT_EQ(liftData1.valueBits, 10);
  EXPECT_EQ(liftData0.valueSquaredBits, 15);
  EXPECT_EQ(liftData1.valueSquaredBits, 15);
}

struct RevealedGroupIds {
  std::vector<uint64_t> groupIds;
  std::vector<uint64_t> testGroupIds;
};

RevealedGroupIds runComputeIndexShares(
    LiftGameProcessedData<0>& publisherOutput,
    LiftGameProcessedData<1>& partnerOutput,
    const std::vector<uint64_t>& cohortGroupIds,
    const std::vector<bool>& breakdownGroupIds,
    const std::vector<bool>& controlPopulation) {
  auto future0 =
      std::async([&publisherOutput, &breakdownGroupIds, &controlPopulation]() {
        SecGroup<0> secCohortGroupIds =
            SecGroup<0>(std::vector<uint64_t>(publisherOutput.numRows), 1);
        SecBit<0> secControlPop = SecBit<0>(controlPopulation, 0);
        SecBit<0> secBreakdownIds = SecBit<0>(breakdownGroupIds, 0);

        SecGroup<0> secTestGroupIds;
        input_processing::computeIndexSharesAndSetTestGroupIds(
            publisherOutput,
            secCohortGroupIds,
            secControlPop,
            secBreakdownIds,
            secTestGroupIds);

        input_processing::computeTestIndexShares(
            publisherOutput, secControlPop, secTestGroupIds);
      });

  auto future1 = std::async([&partnerOutput, &cohortGroupIds]() {
    SecGroup<1> secCohortGroupIds = SecGroup<1>(cohortGroupIds, 1);
    SecBit<1> secControlPop =
        SecBit<1>(std::vector<bool>(partnerOutput.numRows), 0);
    SecBit<1> secBreakdownIds =
        SecBit<1>(std::vector<bool>(partnerOutput.numRows), 0);

    SecGroup<1> secTestGroupIds;

    input_processing::computeIndexSharesAndSetTestGroupIds(
        partnerOutput,
        secCohortGroupIds,
        secControlPop,
        secBreakdownIds,
        secTestGroupIds);

    input_processing::computeTestIndexShares(
        partnerOutput, secControlPop, secTestGroupIds);
  });

  future0.get();
  future1.get();

  RevealedGroupIds results;
  results.groupIds = std::vector<uint64_t>(publisherOutput.numRows, 0);
  results.testGroupIds = std::vector<uint64_t>(publisherOutput.numRows, 0);

  for (int i = 0; i < publisherOutput.numRows; i++) {
    for (int j = 0; j < publisherOutput.indexShares.size(); j++) {
      results.groupIds[i] |= ((uint64_t)(publisherOutput.indexShares[j][i] ^
                                         partnerOutput.indexShares[j][i]))
          << j;
    }
  }

  for (int i = 0; i < publisherOutput.numRows; i++) {
    for (int j = 0; j < publisherOutput.testIndexShares.size(); j++) {
      results.testGroupIds[i] |=
          ((uint64_t)(publisherOutput.testIndexShares[j][i] ^
                      partnerOutput.testIndexShares[j][i]))
          << j;
    }
  }

  return results;
}

RevealedGroupIds computeExpectedResults(
    int numCohorts,
    const std::vector<uint64_t>& cohortGroupIds,
    const std::vector<bool>& breakdownIds,
    const std::vector<bool>& controlPopulation,
    bool usingCohorts,
    bool usingPublisherBreakdowns) {
  auto numRows = cohortGroupIds.size();
  std::vector<uint64_t> expectedGroupIds(numRows);
  std::vector<uint64_t> expectedTestGroupIds(numRows);

  for (int i = 0; i < numRows; i++) {
    if (usingCohorts && usingPublisherBreakdowns) {
      // [0, numCohorts) -> test pop and breakdown 0
      // [numCohorts, 2 * numCohorts) -> test pop and breakdown 1
      // [2 * numCohorts, 3 * numCohorts) -> control pop and breakdown 0
      // [3 * numCohorts, 4*numCohorts) -> control pop and breakdown 1
      expectedGroupIds[i] = cohortGroupIds[i] +
          numCohorts *
              ((controlPopulation[i] ? 2 : 0) + (breakdownIds[i] ? 1 : 0));

      // [0, numCohorts) -> test pop and breakdown 0
      // [numCohorts, 2 * numCohorts] -> test pop and breakdown 1
      // 2 * numCohorts -> control pop
      expectedTestGroupIds[i] = controlPopulation[i]
          ? (2 * numCohorts)
          : (cohortGroupIds[i] + numCohorts * (breakdownIds[i] ? 1 : 0));

    } else if (usingCohorts && !usingPublisherBreakdowns) {
      // [0, numCohorts] -> test pop
      // [numCohorts, 2 * numCohorts] -> controlPop
      expectedGroupIds[i] =
          cohortGroupIds[i] + (controlPopulation[i] ? numCohorts : 0);

      // [0, numCohorts) -> test pop
      // numCohorts -> controlPop
      expectedTestGroupIds[i] =
          controlPopulation[i] ? numCohorts : cohortGroupIds[i];
    } else if (!usingCohorts && usingPublisherBreakdowns) {
      // 0 -> test pop and breakdown 0
      // 1 -> test pop and breakdown 1
      // 2 -> control pop and breakdown 0
      // 3 -> control pop and breakdown 1
      expectedGroupIds[i] =
          ((controlPopulation[i] ? 2 : 0) + (breakdownIds[i] ? 1 : 0));

      // 0 -> test pop and breakdown 0
      // 1 -> test pop and breakdown 1
      // 2 -> control pop
      expectedTestGroupIds[i] =
          controlPopulation[i] ? 2 : (breakdownIds[i] ? 1 : 0);
    } else {
      expectedGroupIds[i] = controlPopulation[i] ? 1 : 0;
      expectedTestGroupIds[i] = controlPopulation[i] ? 1 : 0;
    }
  }

  return RevealedGroupIds{
      .groupIds = expectedGroupIds, .testGroupIds = expectedTestGroupIds};
}

class GlobalSharingUtilsIndexSharesTestFixture
    : public ::testing::TestWithParam<std::tuple<bool, bool>> {};

TEST_P(
    GlobalSharingUtilsIndexSharesTestFixture,
    testGroupIdAndTestGroupCalculation) {
  auto communicationAgentFactory =
      fbpcf::engine::communication::getInMemoryAgentFactory(2);
  fbpcf::setupRealBackend<0, 1>(
      *communicationAgentFactory[0], *communicationAgentFactory[1]);

  std::random_device rd;
  std::mt19937_64 e(rd());
  std::uniform_int_distribution<int32_t> randomRows(50, 100);
  std::uniform_int_distribution<uint8_t> randomBool(0, 1);
  std::uniform_int_distribution<uint8_t> randomNumCohorts(2, 5);

  auto numRows = randomRows(e);
  uint32_t numCohorts = randomNumCohorts(e);

  std::vector<bool> controlPopulation(numRows, false);
  for (size_t i = 0; i < numRows; i++) {
    controlPopulation[i] = randomBool(e);
  }

  std::vector<uint64_t> cohortGroupIds(numRows, 0);
  bool usingCohorts = std::get<0>(GetParam());
  if (usingCohorts) {
    std::uniform_int_distribution<uint32_t> randomCohort(0, numCohorts - 1);

    for (size_t i = 0; i < numRows; i++) {
      cohortGroupIds[i] = randomCohort(e);
    }
  }

  std::vector<bool> breakdownIds(numRows, false);

  bool usingPublisherBreakdowns = std::get<1>(GetParam());
  if (usingPublisherBreakdowns) {
    for (size_t i = 0; i < numRows; i++) {
      breakdownIds[i] = randomBool(e);
    }
  }

  uint32_t numGroups =
      2 * (usingCohorts ? numCohorts : 1) * (usingPublisherBreakdowns ? 2 : 1);
  uint32_t numTestGroups = 1 + numGroups / 2;

  LiftGameProcessedData<0> liftData0{
      .numRows = numRows,
      .numPartnerCohorts = usingCohorts ? numCohorts : 0,
      .numPublisherBreakdowns = usingPublisherBreakdowns ? 2u : 0,
      .numGroups = numGroups,
      .numTestGroups = numTestGroups};
  LiftGameProcessedData<1> liftData1{
      .numRows = numRows,
      .numPartnerCohorts = usingCohorts ? numCohorts : 0,
      .numPublisherBreakdowns = usingPublisherBreakdowns ? 2u : 0,
      .numGroups = numGroups,
      .numTestGroups = numTestGroups};

  auto results = runComputeIndexShares(
      liftData0, liftData1, cohortGroupIds, breakdownIds, controlPopulation);

  auto expectedResults = computeExpectedResults(
      numCohorts,
      cohortGroupIds,
      breakdownIds,
      controlPopulation,
      usingCohorts,
      usingPublisherBreakdowns);

  EXPECT_EQ(results.groupIds.size(), numRows);
  EXPECT_EQ(results.testGroupIds.size(), numRows);
  EXPECT_EQ(results.groupIds, expectedResults.groupIds);
  EXPECT_EQ(results.testGroupIds, expectedResults.testGroupIds);
}

INSTANTIATE_TEST_SUITE_P(
    GlobalSharingUtilsTest,
    GlobalSharingUtilsIndexSharesTestFixture,
    ::testing::Combine(::testing::Bool(), ::testing::Bool()),
    [](const testing::TestParamInfo<
        GlobalSharingUtilsIndexSharesTestFixture::ParamType>& info) {
      bool usingCohorts = std::get<0>(info.param);
      bool usingPublisherBreakdowns = std::get<1>(info.param);
      return folly::sformat(
          "UsingCohorts_{}_UsingPublisherBreakdowns_{}",
          usingCohorts,
          usingPublisherBreakdowns);
    });
} // namespace private_lift
