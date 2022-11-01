/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <gtest/gtest.h>
#include <fstream>
#include <functional>
#include <utility>
#include "folly/Random.h"

#include "fbpcf/engine/communication/test/AgentFactoryCreationHelper.h"
#include "fbpcf/scheduler/ISchedulerFactory.h"
#include "fbpcf/scheduler/SchedulerHelper.h"
#include "fbpcf/test/TestHelper.h"

#include "fbpcs/data_processing/unified_data_process/adapter/AdapterFactory.h"
#include "fbpcs/data_processing/unified_data_process/data_processor/DataProcessorFactory.h"

#include "fbpcs/emp_games/common/Util.h"
#include "fbpcs/emp_games/lift/pcf2_calculator/input_processing/CompactionBasedInputProcessor.h"
#include "fbpcs/emp_games/lift/pcf2_calculator/input_processing/test/TestUtil.h"
#include "fbpcs/emp_games/lift/pcf2_calculator/sample_input/SampleInput.h"

namespace private_lift {

template <int schedulerId>
CompactionBasedInputProcessor<schedulerId> createInputProcessorWithScheduler(
    int myRole,
    InputData inputData,
    int numConversionsPerUser,
    std::reference_wrapper<fbpcf::scheduler::ISchedulerFactory<true>>
        schedulerFactory,
    std::unique_ptr<
        fbpcf::engine::communication::IPartyCommunicationAgentFactory>
        agentFactory) {
  auto scheduler = schedulerFactory.get().create();
  fbpcf::scheduler::SchedulerKeeper<schedulerId>::setScheduler(
      std::move(scheduler));
  int partnerParty =
      myRole == common::PUBLISHER ? common::PARTNER : common::PUBLISHER;
  auto adapter = unified_data_process::adapter::
                     getAdapterFactoryWithAsWaksmanBasedShuffler<schedulerId>(
                         myRole == common::PUBLISHER, myRole, partnerParty)
                         ->create();
  auto dataProcessor =
      unified_data_process::data_processor::getDataProcessorFactoryWithAesCtr<
          schedulerId>(myRole, partnerParty, *agentFactory)
          ->create();
  auto prg = std::make_unique<fbpcf::engine::util::AesPrgFactory>()->create(
      fbpcf::engine::util::getRandomM128iFromSystemNoise());
  return CompactionBasedInputProcessor<schedulerId>(
      myRole,
      std::move(adapter),
      std::move(dataProcessor),
      std::move(prg),
      inputData,
      numConversionsPerUser);
}

class CompactionBasedInputProcessorTest
    : public ::testing::TestWithParam<bool> {
 protected:
  LiftGameProcessedData<0> publisherProcessedData_;
  LiftGameProcessedData<1> partnerProcessedData_;

  bool computePublisherBreakdowns_;

  void SetUp() override {
    std::string publisherInputFilename =
        sample_input::getPublisherInput3().native();
    std::string partnerInputFilename =
        sample_input::getPartnerInput2().native();

    int numConversionsPerUser = 2;
    int epoch = 1546300800;
    computePublisherBreakdowns_ = GetParam();
    auto publisherInputData = InputData(
        publisherInputFilename,
        InputData::LiftMPCType::Standard,
        computePublisherBreakdowns_,
        epoch,
        numConversionsPerUser);
    auto partnerInputData = InputData(
        partnerInputFilename,
        InputData::LiftMPCType::Standard,
        computePublisherBreakdowns_,
        epoch,
        numConversionsPerUser);

    auto factories = fbpcf::engine::communication::getInMemoryAgentFactory(2);
    auto factories2 = fbpcf::engine::communication::getInMemoryAgentFactory(2);

    auto schedulerFactory0 =
        fbpcf::scheduler::NetworkPlaintextSchedulerFactory<true>(
            0, *factories[0]);

    auto schedulerFactory1 =
        fbpcf::scheduler::NetworkPlaintextSchedulerFactory<true>(
            1, *factories[1]);

    auto future0 = std::async(
        createInputProcessorWithScheduler<0>,
        0,
        publisherInputData,
        numConversionsPerUser,
        std::reference_wrapper<fbpcf::scheduler::ISchedulerFactory<true>>(
            schedulerFactory0),
        std::move(factories2[0]));

    auto future1 = std::async(
        createInputProcessorWithScheduler<1>,
        1,
        partnerInputData,
        numConversionsPerUser,
        std::reference_wrapper<fbpcf::scheduler::ISchedulerFactory<true>>(
            schedulerFactory1),
        std::move(factories2[1]));

    publisherProcessedData_ = future0.get().getLiftGameProcessedData();
    partnerProcessedData_ = future1.get().getLiftGameProcessedData();
  }
};

TEST_P(CompactionBasedInputProcessorTest, testNumRows) {
  util::assertNumRows(publisherProcessedData_);
  util::assertNumRows(partnerProcessedData_);
}

TEST_P(CompactionBasedInputProcessorTest, testBitsForValues) {
  util::assertValueBits(publisherProcessedData_);
  util::assertValueBits(partnerProcessedData_);
}

TEST_P(CompactionBasedInputProcessorTest, testNumPartnerCohorts) {
  util::assertPartnerCohorts(publisherProcessedData_);
  util::assertPartnerCohorts(partnerProcessedData_);
}

TEST_P(CompactionBasedInputProcessorTest, testNumBreakdowns) {
  util::assertNumBreakdowns(
      publisherProcessedData_, computePublisherBreakdowns_);
  util::assertNumBreakdowns(partnerProcessedData_, computePublisherBreakdowns_);
}

TEST_P(CompactionBasedInputProcessorTest, testNumGroups) {
  util::assertNumGroups(publisherProcessedData_, computePublisherBreakdowns_);
  util::assertNumGroups(partnerProcessedData_, computePublisherBreakdowns_);
}

TEST_P(CompactionBasedInputProcessorTest, testNumTestGroups) {
  util::assertNumTestGroups(
      publisherProcessedData_, computePublisherBreakdowns_);
  util::assertNumTestGroups(partnerProcessedData_, computePublisherBreakdowns_);
}

TEST_P(CompactionBasedInputProcessorTest, testIndexShares) {
  util::assertIndexShares(
      publisherProcessedData_, computePublisherBreakdowns_, true);
}

TEST_P(CompactionBasedInputProcessorTest, testTestIndexShares) {
  util::assertTestIndexShares(
      publisherProcessedData_, computePublisherBreakdowns_, true);
}

TEST_P(CompactionBasedInputProcessorTest, testOpportunityTimestamps) {
  util::assertOpportunityTimestamps(
      publisherProcessedData_, partnerProcessedData_, true);
}

TEST_P(CompactionBasedInputProcessorTest, testIsValidOpportunityTimestamp) {
  util::assertOpportunityTimestamps(
      publisherProcessedData_, partnerProcessedData_, true);
}

TEST_P(CompactionBasedInputProcessorTest, testPurchaseTimestamps) {
  util::assertPurchaseTimestamps(
      publisherProcessedData_, partnerProcessedData_, true);
}

TEST_P(CompactionBasedInputProcessorTest, testThresholdTimestamps) {
  util::assertThresholdTimestamps(
      publisherProcessedData_, partnerProcessedData_, true);
}

TEST_P(CompactionBasedInputProcessorTest, testAnyValidPurchaseTimestamp) {
  util::assertAnyValidPurchaseTimestamp(
      publisherProcessedData_, partnerProcessedData_, true);
}

TEST_P(CompactionBasedInputProcessorTest, testPurchaseValues) {
  util::assertPurchaseValues(
      publisherProcessedData_, partnerProcessedData_, true);
}

TEST_P(CompactionBasedInputProcessorTest, testPurchaseValueSquared) {
  util::assertPurchaseValuesSquared(
      publisherProcessedData_, partnerProcessedData_, true);
}

TEST_P(CompactionBasedInputProcessorTest, testReach) {
  util::assertReach(publisherProcessedData_, partnerProcessedData_, true);
}

INSTANTIATE_TEST_SUITE_P(
    CompactionBasedInputProcessorTestSuite,
    CompactionBasedInputProcessorTest,
    ::testing::Bool(),
    [](const testing::TestParamInfo<
        CompactionBasedInputProcessorTest::ParamType>& info) {
      std::string computePublisherBreakdowns = info.param ? "True" : "False";
      std::string name =
          "computePublisherBreakdowns_" + computePublisherBreakdowns;
      return name;
    });

} // namespace private_lift
