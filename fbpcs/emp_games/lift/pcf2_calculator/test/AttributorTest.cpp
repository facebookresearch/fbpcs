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
    bool computePublisherBreakdowns = true;
    auto publisherInputData = InputData(
        publisherInputFilename,
        InputData::LiftMPCType::Standard,
        computePublisherBreakdowns,
        epoch,
        numConversionsPerUser);
    auto partnerInputData = InputData(
        partnerInputFilename,
        InputData::LiftMPCType::Standard,
        computePublisherBreakdowns,
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

TEST_F(AttributorTest, testConverters) {
  auto future0 = std::async([&] {
    return publisherAttributor_->getConverters().openToParty(0).getValue();
  });
  auto future1 = std::async([&] {
    return partnerAttributor_->getConverters().openToParty(0).getValue();
  });
  auto converters0 = future0.get();
  auto converters1 = future1.get();
  std::vector<bool> expectConverters = {0, 0, 0, 0, 0, 0, 0, 1, 1, 0, 0,
                                        0, 0, 1, 1, 0, 1, 1, 0, 0, 0, 0,
                                        0, 0, 0, 0, 1, 1, 1, 0, 0, 1, 1};
  EXPECT_EQ(converters0, expectConverters);
}

TEST_F(AttributorTest, testNumConvSquared) {
  auto future0 = std::async([&] {
    return publisherAttributor_->getNumConvSquared().openToParty(0).getValue();
  });
  auto future1 = std::async([&] {
    return partnerAttributor_->getNumConvSquared().openToParty(0).getValue();
  });
  auto numConvSquared0 = future0.get();
  auto numConvSquared1 = future1.get();
  std::vector<uint64_t> expectNumConvSquared = {
      0, 0, 0, 0, 0, 0, 0, 1, 1, 0, 0, 0, 0, 4, 4, 0, 1,
      1, 0, 0, 0, 0, 0, 0, 0, 0, 1, 4, 1, 0, 0, 1, 1};
  EXPECT_EQ(numConvSquared0, expectNumConvSquared);
}

TEST_F(AttributorTest, testMatch) {
  auto future0 = std::async([&] {
    return publisherAttributor_->getMatch().openToParty(0).getValue();
  });
  auto future1 = std::async(
      [&] { return partnerAttributor_->getMatch().openToParty(0).getValue(); });
  auto match0 = future0.get();
  auto match1 = future1.get();
  std::vector<bool> expectMatch = {0, 0, 0, 0, 1, 1, 0, 1, 1, 0, 1,
                                   1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0,
                                   0, 0, 0, 1, 1, 1, 1, 1, 0, 1, 1};
  EXPECT_EQ(match0, expectMatch);
}

template <int schedulerId>
std::vector<std::vector<bool>> revealReachedConversions(
    std::unique_ptr<Attributor<schedulerId>> attributor) {
  std::vector<std::vector<bool>> output;
  for (const auto& reachedConversions : attributor->getReachedConversions()) {
    output.push_back(std::move(reachedConversions.openToParty(0).getValue()));
  }
  return output;
}

TEST_F(AttributorTest, testReachedConversions) {
  auto future0 =
      std::async(revealReachedConversions<0>, std::move(publisherAttributor_));
  auto future1 =
      std::async(revealReachedConversions<1>, std::move(partnerAttributor_));
  auto reachedConversions0 = future0.get();
  auto reachedConversions1 = future1.get();
  std::vector<std::vector<bool>> expectReachedConversions = {
      {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
       0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0},
      {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
       0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 1, 0, 0, 0, 0}};
  EXPECT_EQ(reachedConversions0, expectReachedConversions);
}

template <int schedulerId>
std::vector<std::vector<int64_t>> revealValues(
    std::unique_ptr<Attributor<schedulerId>> attributor) {
  std::vector<std::vector<int64_t>> output;
  for (const auto& value : attributor->getValues()) {
    output.push_back(std::move(value.openToParty(0).getValue()));
  }
  return output;
}

TEST_F(AttributorTest, testValues) {
  auto future0 = std::async(revealValues<0>, std::move(publisherAttributor_));
  auto future1 = std::async(revealValues<1>, std::move(partnerAttributor_));
  auto values0 = future0.get();
  auto values1 = future1.get();
  std::vector<std::vector<int64_t>> expectValues = {
      {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,  0, 0, 10, 10, 0, 0,
       0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 10, 0, 0, 0,  0,  0},
      {0,  0, 0, 0, 0, 0, 0, 20, 20, 0,  0,  0,  0, 20, 20,  0,  20,
       20, 0, 0, 0, 0, 0, 0, 0,  0,  50, 20, 20, 0, 0,  -50, -50}};
  EXPECT_EQ(values0, expectValues);
}

template <int schedulerId>
std::vector<std::vector<int64_t>> revealReachedValues(
    std::unique_ptr<Attributor<schedulerId>> attributor) {
  std::vector<std::vector<int64_t>> output;
  for (const auto& values : attributor->getReachedValues()) {
    output.push_back(std::move(values.openToParty(0).getValue()));
  }
  return output;
}

TEST_F(AttributorTest, testReachedValues) {
  auto future0 =
      std::async(revealReachedValues<0>, std::move(publisherAttributor_));
  auto future1 =
      std::async(revealReachedValues<1>, std::move(partnerAttributor_));
  auto values0 = future0.get();
  auto values1 = future1.get();
  std::vector<std::vector<int64_t>> expectReachedValues = {
      {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,  0, 0, 0, 0, 0, 0,
       0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 10, 0, 0, 0, 0, 0},
      {0, 0, 0, 0, 0, 0, 0, 0, 0, 0,  0,  0,  0, 0, 0, 0, 0,
       0, 0, 0, 0, 0, 0, 0, 0, 0, 50, 20, 20, 0, 0, 0, 0}};
  EXPECT_EQ(values0, expectReachedValues);
}

TEST_F(AttributorTest, testValueSquared) {
  auto future0 = std::async([&] {
    return publisherAttributor_->getValueSquared().openToParty(0).getValue();
  });
  auto future1 = std::async([&] {
    return partnerAttributor_->getValueSquared().openToParty(0).getValue();
  });
  auto values0 = future0.get();
  auto values1 = future1.get();
  std::vector<int64_t> expectValueSquared = {
      0,   0, 0, 0, 0, 0, 0, 400, 400, 0,    0,   0,   0, 900, 900,  0,   400,
      400, 0, 0, 0, 0, 0, 0, 0,   0,   2500, 900, 400, 0, 0,   2500, 2500};
  EXPECT_EQ(values0, expectValueSquared);
}

} // namespace private_lift
