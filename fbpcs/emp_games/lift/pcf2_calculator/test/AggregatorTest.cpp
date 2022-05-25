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
        epoch,
        numConversionsPerUser);
    auto partnerInputData = InputData(
        partnerInputFilename,
        InputData::LiftMPCType::Standard,
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

TEST_F(AggregatorTest, testConverters) {
  auto test = publisherAggregator_->getMetrics().testConverters;
  auto control = publisherAggregator_->getMetrics().controlConverters;
  EXPECT_EQ(test, 7);
  EXPECT_EQ(control, 4);
  auto cohort = publisherAggregator_->getCohortMetrics();
  EXPECT_EQ(cohort[0].testConverters, 2);
  EXPECT_EQ(cohort[1].testConverters, 2);
  EXPECT_EQ(cohort[2].testConverters, 3);
  EXPECT_EQ(cohort[0].controlConverters, 2);
  EXPECT_EQ(cohort[1].controlConverters, 1);
  EXPECT_EQ(cohort[2].controlConverters, 1);
}

TEST_F(AggregatorTest, testNumConvSquared) {
  auto test = publisherAggregator_->getMetrics().testNumConvSquared;
  EXPECT_EQ(test, 13);
  auto control = publisherAggregator_->getMetrics().controlNumConvSquared;
  EXPECT_EQ(control, 7);
  auto cohort = publisherAggregator_->getCohortMetrics();
  EXPECT_EQ(cohort[0].testNumConvSquared, 2);
  EXPECT_EQ(cohort[1].testNumConvSquared, 5);
  EXPECT_EQ(cohort[2].testNumConvSquared, 6);
  EXPECT_EQ(cohort[0].controlNumConvSquared, 2);
  EXPECT_EQ(cohort[1].controlNumConvSquared, 4);
  EXPECT_EQ(cohort[2].controlNumConvSquared, 1);
}

TEST_F(AggregatorTest, testMatchCount) {
  auto test = publisherAggregator_->getMetrics().testMatchCount;
  auto control = publisherAggregator_->getMetrics().controlMatchCount;
  EXPECT_EQ(test, 12);
  EXPECT_EQ(control, 7);
  auto cohort = publisherAggregator_->getCohortMetrics();
  EXPECT_EQ(cohort[0].testMatchCount, 6);
  EXPECT_EQ(cohort[1].testMatchCount, 3);
  EXPECT_EQ(cohort[2].testMatchCount, 3);
  EXPECT_EQ(cohort[0].controlMatchCount, 4);
  EXPECT_EQ(cohort[1].controlMatchCount, 2);
  EXPECT_EQ(cohort[2].controlMatchCount, 1);
}

TEST_F(AggregatorTest, testReachedConversions) {
  auto reachedConversions =
      publisherAggregator_->getMetrics().reachedConversions;
  EXPECT_EQ(reachedConversions, 4);
  auto cohort = publisherAggregator_->getCohortMetrics();
  EXPECT_EQ(cohort[0].reachedConversions, 1);
  EXPECT_EQ(cohort[1].reachedConversions, 0);
  EXPECT_EQ(cohort[2].reachedConversions, 3);
}

TEST_F(AggregatorTest, testValues) {
  auto test = publisherAggregator_->getMetrics().testValue;
  auto control = publisherAggregator_->getMetrics().controlValue;
  EXPECT_EQ(test, 120);
  EXPECT_EQ(control, 20);
  auto cohort = publisherAggregator_->getCohortMetrics();
  EXPECT_EQ(cohort[0].testValue, 40);
  EXPECT_EQ(cohort[1].testValue, 50);
  EXPECT_EQ(cohort[2].testValue, 30);
  EXPECT_EQ(cohort[0].controlValue, 40);
  EXPECT_EQ(cohort[1].controlValue, 30);
  EXPECT_EQ(cohort[2].controlValue, -50);
}

TEST_F(AggregatorTest, testReachedValues) {
  auto test = publisherAggregator_->getMetrics().reachedValue;
  EXPECT_EQ(test, 100);
  auto cohort = publisherAggregator_->getCohortMetrics();
  EXPECT_EQ(cohort[0].reachedValue, 20);
  EXPECT_EQ(cohort[1].reachedValue, 0);
  EXPECT_EQ(cohort[2].reachedValue, 80);
}

TEST_F(AggregatorTest, testValueSquared) {
  auto test = publisherAggregator_->getMetrics().testValueSquared;
  auto control = publisherAggregator_->getMetrics().controlValueSquared;
  EXPECT_EQ(test, 8000);
  EXPECT_EQ(control, 4200);
  auto cohort = publisherAggregator_->getCohortMetrics();
  EXPECT_EQ(cohort[0].testValueSquared, 800);
  EXPECT_EQ(cohort[1].testValueSquared, 1300);
  EXPECT_EQ(cohort[2].testValueSquared, 5900);
  EXPECT_EQ(cohort[0].controlValueSquared, 800);
  EXPECT_EQ(cohort[1].controlValueSquared, 900);
  EXPECT_EQ(cohort[2].controlValueSquared, 2500);
}

} // namespace private_lift
