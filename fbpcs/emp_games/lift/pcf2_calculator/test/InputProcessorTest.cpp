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
#include "fbpcs/emp_games/common/Util.h"
#include "fbpcs/emp_games/lift/pcf2_calculator/InputProcessor.h"

namespace private_lift {
const bool unsafe = true;

template <int schedulerId>
InputProcessor<schedulerId> createInputProcessorWithScheduler(
    int myRole,
    InputData inputData,
    int numConversionsPerUser,
    std::reference_wrapper<
        fbpcf::engine::communication::IPartyCommunicationAgentFactory> factory,
    fbpcf::SchedulerCreator schedulerCreator) {
  auto scheduler = schedulerCreator(myRole, factory);
  fbpcf::scheduler::SchedulerKeeper<schedulerId>::setScheduler(
      std::move(scheduler));
  return InputProcessor<schedulerId>(myRole, inputData, numConversionsPerUser);
}

class InputProcessorTest : public ::testing::TestWithParam<bool> {
 protected:
  InputProcessor<0> publisherInputProcessor_;
  InputProcessor<1> partnerInputProcessor_;
  bool computePublisherBreakdowns_;

  void SetUp() override {
    std::string baseDir =
        private_measurement::test_util::getBaseDirFromPath(__FILE__);
    std::string publisherInputFilename =
        baseDir + "../sample_input/publisher_unittest3.csv";
    std::string partnerInputFilename =
        baseDir + "../sample_input/partner_2_convs_unittest.csv";
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

    auto schedulerCreator =
        fbpcf::scheduler::createNetworkPlaintextScheduler<unsafe>;
    auto factories = fbpcf::engine::communication::getInMemoryAgentFactory(2);

    auto future0 = std::async(
        createInputProcessorWithScheduler<0>,
        0,
        publisherInputData,
        numConversionsPerUser,
        std::reference_wrapper<
            fbpcf::engine::communication::IPartyCommunicationAgentFactory>(
            *factories[0]),
        schedulerCreator);

    auto future1 = std::async(
        createInputProcessorWithScheduler<1>,
        1,
        partnerInputData,
        numConversionsPerUser,
        std::reference_wrapper<
            fbpcf::engine::communication::IPartyCommunicationAgentFactory>(
            *factories[1]),
        schedulerCreator);

    publisherInputProcessor_ = future0.get();
    partnerInputProcessor_ = future1.get();
  }
};

TEST_P(InputProcessorTest, testNumRows) {
  EXPECT_EQ(publisherInputProcessor_.getNumRows(), 33);
  EXPECT_EQ(partnerInputProcessor_.getNumRows(), 33);
}

TEST_P(InputProcessorTest, testBitsForValues) {
  EXPECT_EQ(publisherInputProcessor_.getValueBits(), 10);
  EXPECT_EQ(partnerInputProcessor_.getValueBits(), 10);
  EXPECT_EQ(publisherInputProcessor_.getValueSquaredBits(), 15);
  EXPECT_EQ(partnerInputProcessor_.getValueSquaredBits(), 15);
}

TEST_P(InputProcessorTest, testNumPartnerCohorts) {
  EXPECT_EQ(publisherInputProcessor_.getNumPartnerCohorts(), 3);
  EXPECT_EQ(partnerInputProcessor_.getNumPartnerCohorts(), 3);
}

TEST_P(InputProcessorTest, testNumBreakdowns) {
  if (computePublisherBreakdowns_) {
    EXPECT_EQ(publisherInputProcessor_.getNumPublisherBreakdowns(), 2);
    EXPECT_EQ(partnerInputProcessor_.getNumPublisherBreakdowns(), 2);
  } else {
    EXPECT_EQ(publisherInputProcessor_.getNumPublisherBreakdowns(), 0);
    EXPECT_EQ(partnerInputProcessor_.getNumPublisherBreakdowns(), 0);
  }
}

TEST_P(InputProcessorTest, testNumGroups) {
  if (computePublisherBreakdowns_) {
    EXPECT_EQ(publisherInputProcessor_.getNumGroups(), 12);
    EXPECT_EQ(partnerInputProcessor_.getNumGroups(), 12);
  } else {
    EXPECT_EQ(publisherInputProcessor_.getNumGroups(), 6);
    EXPECT_EQ(partnerInputProcessor_.getNumGroups(), 6);
  }
}

TEST_P(InputProcessorTest, testNumTestGroups) {
  if (computePublisherBreakdowns_) {
    EXPECT_EQ(publisherInputProcessor_.getNumTestGroups(), 7);
    EXPECT_EQ(partnerInputProcessor_.getNumTestGroups(), 7);
  } else {
    EXPECT_EQ(publisherInputProcessor_.getNumTestGroups(), 4);
    EXPECT_EQ(partnerInputProcessor_.getNumTestGroups(), 4);
  }
}

// Convert input boolean index shares to group ids
std::vector<uint32_t> convertIndexSharesToGroupIds(
    std::vector<std::vector<bool>> indexShares) {
  std::vector<uint32_t> groupIds;
  if (indexShares.size() == 0) {
    return groupIds;
  }
  for (auto i = 0; i < indexShares.at(0).size(); ++i) {
    uint32_t groupId = 0;
    for (auto j = 0; j < indexShares.size(); ++j) {
      groupId += indexShares.at(j).at(i) << j;
    }
    groupIds.push_back(groupId);
  }
  return groupIds;
}

TEST_P(InputProcessorTest, testIndexShares) {
  auto publisherShares = publisherInputProcessor_.getIndexShares();
  size_t groupWidth =
      std::ceil(std::log2(publisherInputProcessor_.getNumGroups()));
  EXPECT_EQ(publisherShares.size(), groupWidth);
  std::vector<uint32_t> expectGroupIds;
  if (computePublisherBreakdowns_) {
    expectGroupIds = {3, 1, 9, 0, 0, 7, 1, 4, 6, 1, 4, 6, 3, 1, 7, 3, 3,
                      6, 0, 0, 6, 3, 3, 6, 3, 0, 2, 5, 3, 3, 5, 2, 11};
  } else {
    expectGroupIds = {0, 1, 3, 0, 0, 4, 1, 1, 3, 1, 1, 3, 0, 1, 4, 0, 0,
                      3, 0, 0, 3, 0, 0, 3, 0, 0, 2, 2, 0, 0, 2, 2, 5};
  }
  auto groupIds = convertIndexSharesToGroupIds(publisherShares);
  EXPECT_EQ(expectGroupIds, groupIds);
}

TEST_P(InputProcessorTest, testTestIndexShares) {
  auto publisherShares = publisherInputProcessor_.getTestIndexShares();
  size_t testGroupWidth =
      std::ceil(std::log2(publisherInputProcessor_.getNumTestGroups()));
  EXPECT_EQ(publisherShares.size(), testGroupWidth);
  std::vector<uint32_t> expectTestGroupIds;
  if (computePublisherBreakdowns_) {
    expectTestGroupIds = {3, 1, 6, 0, 0, 6, 1, 4, 6, 1, 4, 6, 3, 1, 6, 3, 3,
                          6, 0, 0, 6, 3, 3, 6, 3, 0, 2, 5, 3, 3, 5, 2, 6};
  } else {
    expectTestGroupIds = {0, 1, 3, 0, 0, 3, 1, 1, 3, 1, 1, 3, 0, 1, 3, 0, 0,
                          3, 0, 0, 3, 0, 0, 3, 0, 0, 2, 2, 0, 0, 2, 2, 3};
  }
  auto testGroupIds = convertIndexSharesToGroupIds(publisherShares);
  EXPECT_EQ(expectTestGroupIds, testGroupIds);
}

TEST_P(InputProcessorTest, testOpportunityTimestamps) {
  auto future0 = std::async([&] {
    return publisherInputProcessor_.getOpportunityTimestamps()
        .openToParty(0)
        .getValue();
  });
  auto future1 = std::async([&] {
    return partnerInputProcessor_.getOpportunityTimestamps()
        .openToParty(0)
        .getValue();
  });
  auto opportunityTimestamps0 = future0.get();
  auto opportunityTimestamps1 = future1.get();
  std::vector<uint64_t> expectOpportunityTimestamps = {
      0,   0,   0,   100, 100, 100, 100, 100, 100, 100, 100,
      100, 100, 100, 100, 100, 100, 100, 100, 100, 100, 100,
      100, 100, 0,   100, 100, 100, 100, 100, 100, 100, 100};
  EXPECT_EQ(opportunityTimestamps0, expectOpportunityTimestamps);
}

TEST_P(InputProcessorTest, testIsValidOpportunityTimestamp) {
  auto future0 = std::async([&] {
    return publisherInputProcessor_.getIsValidOpportunityTimestamp()
        .openToParty(0)
        .getValue();
  });
  auto future1 = std::async([&] {
    return partnerInputProcessor_.getIsValidOpportunityTimestamp()
        .openToParty(0)
        .getValue();
  });
  auto isValidOpportunityTimestamp0 = future0.get();
  auto isValidOpportunityTimestamp1 = future1.get();
  std::vector<bool> expectIsValidOpportunityTimestamp = {
      0, 0, 0, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1,
      1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 1, 1, 1, 0, 1, 1};
  EXPECT_EQ(isValidOpportunityTimestamp0, expectIsValidOpportunityTimestamp);
}

template <int schedulerId>
std::vector<std::vector<uint64_t>> revealPurchaseTimestamps(
    InputProcessor<schedulerId> inputProcessor) {
  std::vector<std::vector<uint64_t>> purchaseTimestamps;
  for (size_t i = 0; i < inputProcessor.getPurchaseTimestamps().size(); ++i) {
    purchaseTimestamps.push_back(
        std::move(inputProcessor.getPurchaseTimestamps()
                      .at(i)
                      .openToParty(0)
                      .getValue()));
  }
  return purchaseTimestamps;
}

TEST_P(InputProcessorTest, testPurchaseTimestamps) {
  auto future0 =
      std::async(revealPurchaseTimestamps<0>, publisherInputProcessor_);
  auto future1 =
      std::async(revealPurchaseTimestamps<1>, partnerInputProcessor_);
  auto purchaseTimestamps0 = future0.get();
  auto purchaseTimestamps1 = future1.get();
  std::vector<std::vector<uint64_t>> expectPurchaseTimestamps = {
      {0,  0,  0,  0,  0, 0, 0, 0, 0, 0, 0,   0,  150, 150, 150, 50, 50,
       50, 30, 30, 30, 0, 0, 0, 0, 0, 0, 150, 50, 30,  0,   0,   0},
      {100, 100, 100, 50,  50,  50,  100, 100, 100, 90,  90,
       90,  200, 200, 200, 150, 150, 150, 50,  50,  50,  0,
       0,   0,   100, 50,  150, 200, 150, 50,  200, 200, 200}};
  EXPECT_EQ(purchaseTimestamps0, expectPurchaseTimestamps);
}

template <int schedulerId>
std::vector<std::vector<uint64_t>> revealThresholdTimestamps(
    InputProcessor<schedulerId> inputProcessor) {
  std::vector<std::vector<uint64_t>> thresholdTimestamps;
  for (size_t i = 0; i < inputProcessor.getThresholdTimestamps().size(); ++i) {
    thresholdTimestamps.push_back(
        std::move(inputProcessor.getThresholdTimestamps()
                      .at(i)
                      .openToParty(0)
                      .getValue()));
  }
  return thresholdTimestamps;
}

TEST_P(InputProcessorTest, testThresholdTimestamps) {
  auto future0 =
      std::async(revealThresholdTimestamps<0>, publisherInputProcessor_);
  auto future1 =
      std::async(revealThresholdTimestamps<1>, partnerInputProcessor_);
  auto thresholdTimestamps0 = future0.get();
  auto thresholdTimestamps1 = future1.get();
  std::vector<std::vector<uint64_t>> expectThresholdTimestamps = {
      {0,  0,  0,  0,  0, 0, 0, 0, 0, 0, 0,   0,  160, 160, 160, 60, 60,
       60, 40, 40, 40, 0, 0, 0, 0, 0, 0, 160, 60, 40,  0,   0,   0},
      {110, 110, 110, 60,  60,  60,  110, 110, 110, 100, 100,
       100, 210, 210, 210, 160, 160, 160, 60,  60,  60,  0,
       0,   0,   110, 60,  160, 210, 160, 60,  210, 210, 210}};
  EXPECT_EQ(thresholdTimestamps0, expectThresholdTimestamps);
}

TEST_P(InputProcessorTest, testAnyValidPurchaseTimestamp) {
  auto future0 = std::async([&] {
    return publisherInputProcessor_.getAnyValidPurchaseTimestamp()
        .openToParty(0)
        .getValue();
  });
  auto future1 = std::async([&] {
    return partnerInputProcessor_.getAnyValidPurchaseTimestamp()
        .openToParty(0)
        .getValue();
  });
  auto anyValidPurchaseTimestamp0 = future0.get();
  auto anyValidPurchaseTimestamp1 = future1.get();
  std::vector<bool> expectAnyValidPurchaseTimestamp = {
      1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
      1, 1, 1, 1, 0, 0, 0, 1, 1, 1, 1, 1, 1, 1, 1, 1};
  EXPECT_EQ(anyValidPurchaseTimestamp0, expectAnyValidPurchaseTimestamp);
}

template <int schedulerId>
std::vector<std::vector<int64_t>> revealPurchaseValues(
    InputProcessor<schedulerId> inputProcessor) {
  std::vector<std::vector<int64_t>> purchaseValues;
  for (size_t i = 0; i < inputProcessor.getPurchaseValues().size(); ++i) {
    purchaseValues.push_back(std::move(
        inputProcessor.getPurchaseValues().at(i).openToParty(0).getValue()));
  }
  return purchaseValues;
}

TEST_P(InputProcessorTest, testPurchaseValues) {
  auto future0 = std::async(revealPurchaseValues<0>, publisherInputProcessor_);
  auto future1 = std::async(revealPurchaseValues<1>, partnerInputProcessor_);
  auto purchaseValues0 = future0.get();
  auto purchaseValues1 = future1.get();
  std::vector<std::vector<int64_t>> expectPurchaseValues = {
      {0,  0,  0,  0,  0, 0, 0, 0, 0, 0, 0,  0,  10, 10, 10, 10, 10,
       10, 10, 10, 10, 0, 0, 0, 0, 0, 0, 10, 10, 10, 0,  0,  0},
      {0,  0,  0,  20, 20, 20, 20, 20, 20, 20, 20, 20, 20, 20,  20,  20, 20,
       20, 20, 20, 20, 0,  0,  0,  50, 50, 50, 20, 20, 20, -50, -50, -50}};
  EXPECT_EQ(purchaseValues0, expectPurchaseValues);
}

template <int schedulerId>
std::vector<std::vector<int64_t>> revealPurchaseValueSquared(
    InputProcessor<schedulerId> inputProcessor) {
  std::vector<std::vector<int64_t>> purchaseValueSquared;
  for (size_t i = 0; i < inputProcessor.getPurchaseValueSquared().size(); ++i) {
    purchaseValueSquared.push_back(
        std::move(inputProcessor.getPurchaseValueSquared()
                      .at(i)
                      .openToParty(0)
                      .getValue()));
  }
  return purchaseValueSquared;
}

TEST_P(InputProcessorTest, testPurchaseValueSquared) {
  auto future0 =
      std::async(revealPurchaseValueSquared<0>, publisherInputProcessor_);
  auto future1 =
      std::async(revealPurchaseValueSquared<1>, partnerInputProcessor_);
  auto purchaseValueSquared0 = future0.get();
  auto purchaseValueSquared1 = future1.get();
  // squared sum of purchase value in each row
  std::vector<std::vector<int64_t>> expectPurchaseValueSquared = {
      {0,   0,   0,    400,  400,  400, 400, 400, 400,  400,  400,
       400, 900, 900,  900,  900,  900, 900, 900, 900,  900,  0,
       0,   0,   2500, 2500, 2500, 900, 900, 900, 2500, 2500, 2500},
      {0,   0,   0,    400,  400,  400, 400, 400, 400,  400,  400,
       400, 400, 400,  400,  400,  400, 400, 400, 400,  400,  0,
       0,   0,   2500, 2500, 2500, 400, 400, 400, 2500, 2500, 2500}};
  EXPECT_EQ(purchaseValueSquared0, expectPurchaseValueSquared);
}

TEST_P(InputProcessorTest, testReach) {
  auto future0 = std::async([&] {
    return publisherInputProcessor_.getTestReach().openToParty(0).getValue();
  });
  auto future1 = std::async([&] {
    return partnerInputProcessor_.getTestReach().openToParty(0).getValue();
  });
  auto testReach0 = future0.get();
  auto testReach1 = future1.get();

  std::vector<bool> expectTestReach = {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                                       0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                                       0, 0, 1, 1, 1, 1, 1, 1, 0, 0, 0};
  EXPECT_EQ(testReach0, expectTestReach);
}

INSTANTIATE_TEST_SUITE_P(
    InputProcessorTestSuite,
    InputProcessorTest,
    ::testing::Bool(),
    [](const testing::TestParamInfo<InputProcessorTest::ParamType>& info) {
      std::string computePublisherBreakdowns = info.param ? "True" : "False";
      std::string name =
          "computePublisherBreakdowns_" + computePublisherBreakdowns;
      return name;
    });

} // namespace private_lift
