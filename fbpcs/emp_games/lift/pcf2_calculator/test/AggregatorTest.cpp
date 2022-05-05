/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <gtest/gtest.h>

#include "fbpcf/engine/communication/test/AgentFactoryCreationHelper.h"
#include "fbpcf/scheduler/SchedulerHelper.h"
#include "fbpcf/test/TestHelper.h"

#include "fbpcs/emp_games/common/TestUtil.h"
#include "fbpcs/emp_games/lift/pcf2_calculator/Aggregator.h"
#include "fbpcs/emp_games/lift/pcf2_calculator/Attributor.h"
#include "fbpcs/emp_games/lift/pcf2_calculator/InputProcessor.h"

namespace private_lift {
const bool unsafe = true;

template <int schedulerId>
Aggregator<schedulerId> createAggregatorWithScheduler(
    int myRole,
    InputData inputData,
    int numConversionsPerUser,
    std::shared_ptr<
        fbpcf::engine::communication::IPartyCommunicationAgentFactory> factory,
    fbpcf::SchedulerCreator schedulerCreator) {
  auto scheduler = schedulerCreator(myRole, *factory);
  fbpcf::scheduler::SchedulerKeeper<schedulerId>::setScheduler(
      std::move(scheduler));
  auto inputProcessor =
      InputProcessor<schedulerId>(myRole, inputData, numConversionsPerUser);
  auto attributor =
      std::make_unique<Attributor<schedulerId>>(myRole, inputProcessor);
  return Aggregator<schedulerId>(
      myRole,
      inputProcessor,
      std::move(attributor),
      numConversionsPerUser,
      factory);
}

class AggregatorTest : public ::testing::Test {
 protected:
  std::unique_ptr<Aggregator<0>> publisherAggregator_;
  std::unique_ptr<Aggregator<1>> partnerAggregator_;

  void SetUp() override {
    std::string baseDir =
        private_measurement::test_util::getBaseDirFromPath(__FILE__);
    std::string publisherInputFilename =
        baseDir + "../sample_input/publisher_unittest3.csv";
    std::string partnerInputFilename =
        baseDir + "../sample_input/partner_2_convs_unittest.csv";
    int numConversionsPerUser = 2;
    int epoch = 1546300800;
    auto publisherInputData = InputData(
        publisherInputFilename,
        InputData::LiftMPCType::Standard,
        InputData::LiftGranularityType::Conversion,
        epoch,
        numConversionsPerUser);
    auto partnerInputData = InputData(
        partnerInputFilename,
        InputData::LiftMPCType::Standard,
        InputData::LiftGranularityType::Conversion,
        epoch,
        numConversionsPerUser);

    auto schedulerCreator =
        fbpcf::scheduler::createNetworkPlaintextScheduler<unsafe>;
    auto factories = fbpcf::engine::communication::getInMemoryAgentFactory(2);

    auto future0 = std::async(
        createAggregatorWithScheduler<0>,
        0,
        publisherInputData,
        numConversionsPerUser,
        std::move(factories[0]),
        schedulerCreator);

    auto future1 = std::async(
        createAggregatorWithScheduler<1>,
        1,
        partnerInputData,
        numConversionsPerUser,
        std::move(factories[1]),
        schedulerCreator);

    publisherAggregator_ = std::make_unique<Aggregator<0>>(future0.get());
    partnerAggregator_ = std::make_unique<Aggregator<1>>(future1.get());
  }
};

TEST_F(AggregatorTest, testEvents) {
  auto test = publisherAggregator_->getMetrics().testEvents;
  auto control = publisherAggregator_->getMetrics().controlEvents;
  EXPECT_EQ(test, 9);
  EXPECT_EQ(control, 5);
  auto cohort = publisherAggregator_->getCohortMetrics();
  EXPECT_EQ(cohort[0].testEvents, 2);
  EXPECT_EQ(cohort[1].testEvents, 3);
  EXPECT_EQ(cohort[2].testEvents, 4);
  EXPECT_EQ(cohort[0].controlEvents, 2);
  EXPECT_EQ(cohort[1].controlEvents, 2);
  EXPECT_EQ(cohort[2].controlEvents, 1);
}

} // namespace private_lift
