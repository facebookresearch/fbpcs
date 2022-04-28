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
#include "fbpcs/emp_games/lift/pcf2_calculator/Attributor.h"
#include "fbpcs/emp_games/lift/pcf2_calculator/InputProcessor.h"

namespace private_lift {
const bool unsafe = true;

template <int schedulerId>
Attributor<schedulerId> createAttributorWithScheduler(
    int myRole,
    InputData inputData,
    int numConversionsPerUser,
    std::reference_wrapper<
        fbpcf::engine::communication::IPartyCommunicationAgentFactory> factory,
    fbpcf::SchedulerCreator schedulerCreator) {
  auto scheduler = schedulerCreator(myRole, factory);
  fbpcf::scheduler::SchedulerKeeper<schedulerId>::setScheduler(
      std::move(scheduler));
  auto inputProcessor =
      InputProcessor<schedulerId>(myRole, inputData, numConversionsPerUser);
  return Attributor<schedulerId>(myRole, inputProcessor);
}

class AttributorTest : public ::testing::Test {
 protected:
  std::unique_ptr<Attributor<0>> publisherAttributor_;
  std::unique_ptr<Attributor<1>> partnerAttributor_;

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
        createAttributorWithScheduler<0>,
        0,
        publisherInputData,
        numConversionsPerUser,
        std::reference_wrapper<
            fbpcf::engine::communication::IPartyCommunicationAgentFactory>(
            *factories[0]),
        schedulerCreator);

    auto future1 = std::async(
        createAttributorWithScheduler<1>,
        1,
        partnerInputData,
        numConversionsPerUser,
        std::reference_wrapper<
            fbpcf::engine::communication::IPartyCommunicationAgentFactory>(
            *factories[1]),
        schedulerCreator);

    publisherAttributor_ = std::make_unique<Attributor<0>>(future0.get());
    partnerAttributor_ = std::make_unique<Attributor<1>>(future1.get());
  }
};

template <int schedulerId>
std::vector<std::vector<bool>> revealEvents(
    std::unique_ptr<Attributor<schedulerId>> attributor) {
  std::vector<std::vector<bool>> output;
  for (const auto& events : attributor->getEvents()) {
    output.push_back(events.openToParty(0).getValue());
  }
  return output;
}

TEST_F(AttributorTest, testEvents) {
  auto future0 = std::async(revealEvents<0>, std::move(publisherAttributor_));
  auto future1 = std::async(revealEvents<1>, std::move(partnerAttributor_));
  auto events0 = future0.get();
  auto events1 = future1.get();
  std::vector<std::vector<bool>> expectEvents = {
      {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 0, 0,
       0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0},
      {0, 0, 0, 0, 0, 0, 0, 1, 1, 0, 0, 0, 0, 1, 1, 0, 1,
       1, 0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 1, 0, 0, 1, 1}};
  EXPECT_EQ(events0, expectEvents);
}

} // namespace private_lift
