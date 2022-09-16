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
    std::reference_wrapper<fbpcf::scheduler::ISchedulerFactory<unsafe>>
        schedulerFactory) {
  auto scheduler = schedulerFactory.get().create();
  fbpcf::scheduler::SchedulerKeeper<schedulerId>::setScheduler(
      std::move(scheduler));
  return InputProcessor<schedulerId>(myRole, inputData, numConversionsPerUser);
}

template <int schedulerId>
void serializeAndDeserializeData(
    std::reference_wrapper<InputProcessor<schedulerId>> inputProcessor,
    std::reference_wrapper<LiftGameProcessedData<schedulerId>> toWrite,
    const std::string& globalParamsPath,
    const std::string& secretSharesPath) {
  inputProcessor.get().getLiftGameProcessedData().writeToCSV(
      globalParamsPath, secretSharesPath);

  toWrite.get() = LiftGameProcessedData<schedulerId>::readFromCSV(
      globalParamsPath, secretSharesPath);
}

static void cleanup(std::string file_to_delete) {
  remove(file_to_delete.c_str());
}

class InputProcessorTest : public ::testing::TestWithParam<bool> {
 protected:
  InputProcessor<0> publisherInputProcessor_;
  InputProcessor<1> partnerInputProcessor_;
  LiftGameProcessedData<0> publisherDeserialized_;
  LiftGameProcessedData<1> partnerDeserialized_;

  bool computePublisherBreakdowns_;

  void SetUp() override {
    std::string baseDir =
        private_measurement::test_util::getBaseDirFromPath(__FILE__);
    std::string publisherInputFilename =
        baseDir + "../sample_input/publisher_unittest3.csv";
    std::string partnerInputFilename =
        baseDir + "../sample_input/partner_2_convs_unittest.csv";

    std::string publisherGlobalParamsOutput = folly::sformat(
        "{}../sample_input/publisher_global_params_{}.json",
        baseDir,
        folly::Random::secureRand64());

    std::string publisherSecretSharesOutput = folly::sformat(
        "{}../sample_input/publisher_secret_shares_{}.json",
        baseDir,
        folly::Random::secureRand64());

    std::string partnerGlobalParamsOutput = folly::sformat(
        "{}../sample_input/partner_global_params_{}.json",
        baseDir,
        folly::Random::secureRand64());

    std::string partnerSecretSharesOutput = folly::sformat(
        "{}../sample_input/partner_secret_shares_{}.json",
        baseDir,
        folly::Random::secureRand64());

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

    auto schedulerFactory0 =
        fbpcf::scheduler::NetworkPlaintextSchedulerFactory<unsafe>(
            0, *factories[0]);

    auto schedulerFactory1 =
        fbpcf::scheduler::NetworkPlaintextSchedulerFactory<unsafe>(
            1, *factories[1]);

    auto future0 = std::async(
        createInputProcessorWithScheduler<0>,
        0,
        publisherInputData,
        numConversionsPerUser,
        std::reference_wrapper<fbpcf::scheduler::ISchedulerFactory<unsafe>>(
            schedulerFactory0));

    auto future1 = std::async(
        createInputProcessorWithScheduler<1>,
        1,
        partnerInputData,
        numConversionsPerUser,
        std::reference_wrapper<fbpcf::scheduler::ISchedulerFactory<unsafe>>(
            schedulerFactory1));

    publisherInputProcessor_ = future0.get();
    partnerInputProcessor_ = future1.get();

    auto future2 = std::async(
        serializeAndDeserializeData<0>,
        std::reference_wrapper<InputProcessor<0>>(publisherInputProcessor_),
        std::reference_wrapper<LiftGameProcessedData<0>>(
            publisherDeserialized_),
        publisherGlobalParamsOutput,
        publisherSecretSharesOutput);

    auto future3 = std::async(
        serializeAndDeserializeData<1>,
        std::reference_wrapper<InputProcessor<1>>(partnerInputProcessor_),
        std::reference_wrapper<LiftGameProcessedData<1>>(partnerDeserialized_),
        partnerGlobalParamsOutput,
        partnerSecretSharesOutput);

    future2.get();
    future3.get();

    cleanup(publisherGlobalParamsOutput);
    cleanup(publisherSecretSharesOutput);
    cleanup(partnerGlobalParamsOutput);
    cleanup(partnerSecretSharesOutput);
  }
};

TEST_P(InputProcessorTest, testNumRows) {
  EXPECT_EQ(publisherInputProcessor_.getLiftGameProcessedData().numRows, 33);
  EXPECT_EQ(partnerInputProcessor_.getLiftGameProcessedData().numRows, 33);

  EXPECT_EQ(
      publisherInputProcessor_.getLiftGameProcessedData().numRows,
      publisherDeserialized_.numRows);

  EXPECT_EQ(
      partnerInputProcessor_.getLiftGameProcessedData().numRows,
      partnerDeserialized_.numRows);
}

TEST_P(InputProcessorTest, testBitsForValues) {
  EXPECT_EQ(publisherInputProcessor_.getLiftGameProcessedData().valueBits, 10);
  EXPECT_EQ(partnerInputProcessor_.getLiftGameProcessedData().valueBits, 10);
  EXPECT_EQ(
      publisherInputProcessor_.getLiftGameProcessedData().valueSquaredBits, 15);
  EXPECT_EQ(
      partnerInputProcessor_.getLiftGameProcessedData().valueSquaredBits, 15);

  EXPECT_EQ(
      publisherInputProcessor_.getLiftGameProcessedData().valueBits,
      publisherDeserialized_.valueBits);
  EXPECT_EQ(
      partnerInputProcessor_.getLiftGameProcessedData().valueBits,
      partnerDeserialized_.valueBits);
  EXPECT_EQ(
      publisherInputProcessor_.getLiftGameProcessedData().valueSquaredBits,
      publisherDeserialized_.valueSquaredBits);
  EXPECT_EQ(
      partnerInputProcessor_.getLiftGameProcessedData().valueSquaredBits,
      partnerDeserialized_.valueSquaredBits);
}

TEST_P(InputProcessorTest, testNumPartnerCohorts) {
  EXPECT_EQ(
      publisherInputProcessor_.getLiftGameProcessedData().numPartnerCohorts, 3);
  EXPECT_EQ(
      partnerInputProcessor_.getLiftGameProcessedData().numPartnerCohorts, 3);

  EXPECT_EQ(
      publisherInputProcessor_.getLiftGameProcessedData().numPartnerCohorts,
      publisherDeserialized_.numPartnerCohorts);
  EXPECT_EQ(
      partnerInputProcessor_.getLiftGameProcessedData().numPartnerCohorts,
      partnerDeserialized_.numPartnerCohorts);
}

TEST_P(InputProcessorTest, testNumBreakdowns) {
  if (computePublisherBreakdowns_) {
    EXPECT_EQ(
        publisherInputProcessor_.getLiftGameProcessedData()
            .numPublisherBreakdowns,
        2);
    EXPECT_EQ(
        partnerInputProcessor_.getLiftGameProcessedData()
            .numPublisherBreakdowns,
        2);
  } else {
    EXPECT_EQ(
        publisherInputProcessor_.getLiftGameProcessedData()
            .numPublisherBreakdowns,
        0);
    EXPECT_EQ(
        partnerInputProcessor_.getLiftGameProcessedData()
            .numPublisherBreakdowns,
        0);
  }

  EXPECT_EQ(
      publisherInputProcessor_.getLiftGameProcessedData()
          .numPublisherBreakdowns,
      publisherDeserialized_.numPublisherBreakdowns);
  EXPECT_EQ(
      partnerInputProcessor_.getLiftGameProcessedData().numPublisherBreakdowns,
      partnerDeserialized_.numPublisherBreakdowns);
}

TEST_P(InputProcessorTest, testNumGroups) {
  if (computePublisherBreakdowns_) {
    EXPECT_EQ(
        publisherInputProcessor_.getLiftGameProcessedData().numGroups, 12);
    EXPECT_EQ(partnerInputProcessor_.getLiftGameProcessedData().numGroups, 12);
  } else {
    EXPECT_EQ(publisherInputProcessor_.getLiftGameProcessedData().numGroups, 6);
    EXPECT_EQ(partnerInputProcessor_.getLiftGameProcessedData().numGroups, 6);
  }

  EXPECT_EQ(
      publisherInputProcessor_.getLiftGameProcessedData().numGroups,
      publisherDeserialized_.numGroups);
  EXPECT_EQ(
      partnerInputProcessor_.getLiftGameProcessedData().numGroups,
      partnerDeserialized_.numGroups);
}

TEST_P(InputProcessorTest, testNumTestGroups) {
  if (computePublisherBreakdowns_) {
    EXPECT_EQ(
        publisherInputProcessor_.getLiftGameProcessedData().numTestGroups, 7);
    EXPECT_EQ(
        partnerInputProcessor_.getLiftGameProcessedData().numTestGroups, 7);
  } else {
    EXPECT_EQ(
        publisherInputProcessor_.getLiftGameProcessedData().numTestGroups, 4);
    EXPECT_EQ(
        partnerInputProcessor_.getLiftGameProcessedData().numTestGroups, 4);
  }

  EXPECT_EQ(
      publisherInputProcessor_.getLiftGameProcessedData().numTestGroups,
      publisherDeserialized_.numTestGroups);
  EXPECT_EQ(
      partnerInputProcessor_.getLiftGameProcessedData().numTestGroups,
      partnerDeserialized_.numTestGroups);
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
  auto publisherShares =
      publisherInputProcessor_.getLiftGameProcessedData().indexShares;
  size_t groupWidth = std::ceil(
      std::log2(publisherInputProcessor_.getLiftGameProcessedData().numGroups));
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
  auto publisherShares =
      publisherInputProcessor_.getLiftGameProcessedData().testIndexShares;
  size_t testGroupWidth = std::ceil(std::log2(
      publisherInputProcessor_.getLiftGameProcessedData().numTestGroups));
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
    return publisherInputProcessor_.getLiftGameProcessedData()
        .opportunityTimestamps.openToParty(0)
        .getValue();
  });
  auto future1 = std::async([&] {
    return partnerInputProcessor_.getLiftGameProcessedData()
        .opportunityTimestamps.openToParty(0)
        .getValue();
  });
  auto opportunityTimestamps0 = future0.get();
  future1.get();
  std::vector<uint64_t> expectOpportunityTimestamps = {
      0,   0,   0,   100, 100, 100, 100, 100, 100, 100, 100,
      100, 100, 100, 100, 100, 100, 100, 100, 100, 100, 100,
      100, 100, 0,   100, 100, 100, 100, 100, 100, 100, 100};
  EXPECT_EQ(opportunityTimestamps0, expectOpportunityTimestamps);

  auto future2 = std::async([&] {
    return publisherDeserialized_.opportunityTimestamps.openToParty(0)
        .getValue();
  });
  auto future3 = std::async([&] {
    return partnerDeserialized_.opportunityTimestamps.openToParty(0).getValue();
  });

  auto deserializedOpportunityTimestamps = future2.get();
  future3.get();

  EXPECT_EQ(opportunityTimestamps0, deserializedOpportunityTimestamps);
}

TEST_P(InputProcessorTest, testIsValidOpportunityTimestamp) {
  auto future0 = std::async([&] {
    return publisherInputProcessor_.getLiftGameProcessedData()
        .isValidOpportunityTimestamp.openToParty(0)
        .getValue();
  });
  auto future1 = std::async([&] {
    return partnerInputProcessor_.getLiftGameProcessedData()
        .isValidOpportunityTimestamp.openToParty(0)
        .getValue();
  });
  auto isValidOpportunityTimestamp0 = future0.get();
  future1.get();
  std::vector<bool> expectIsValidOpportunityTimestamp = {
      0, 0, 0, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1,
      1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 1, 1, 1, 0, 1, 1};
  EXPECT_EQ(isValidOpportunityTimestamp0, expectIsValidOpportunityTimestamp);

  auto future2 = std::async([&] {
    return publisherDeserialized_.isValidOpportunityTimestamp.openToParty(0)
        .getValue();
  });
  auto future3 = std::async([&] {
    return partnerDeserialized_.isValidOpportunityTimestamp.openToParty(0)
        .getValue();
  });

  auto deserializedIsValidOpportunityTimestamp = future2.get();
  future3.get();

  EXPECT_EQ(
      isValidOpportunityTimestamp0, deserializedIsValidOpportunityTimestamp);
}

template <int schedulerId>
std::vector<std::vector<uint64_t>> revealTimestamps(
    std::reference_wrapper<const std::vector<SecTimestamp<schedulerId>>>
        timestamps) {
  std::vector<std::vector<uint64_t>> result;
  for (size_t i = 0; i < timestamps.get().size(); ++i) {
    result.push_back(
        std::move(timestamps.get().at(i).openToParty(0).getValue()));
  }
  return result;
}

TEST_P(InputProcessorTest, testPurchaseTimestamps) {
  auto future0 = std::async(
      revealTimestamps<0>,
      std::reference_wrapper<const std::vector<SecTimestamp<0>>>(
          publisherInputProcessor_.getLiftGameProcessedData()
              .purchaseTimestamps));
  auto future1 = std::async(
      revealTimestamps<1>,
      std::reference_wrapper<const std::vector<SecTimestamp<1>>>(
          partnerInputProcessor_.getLiftGameProcessedData()
              .purchaseTimestamps));
  auto purchaseTimestamps0 = future0.get();
  future1.get();
  std::vector<std::vector<uint64_t>> expectPurchaseTimestamps = {
      {0,  0,  0,  0,  0, 0, 0, 0, 0, 0, 0,   0,  150, 150, 150, 50, 50,
       50, 30, 30, 30, 0, 0, 0, 0, 0, 0, 150, 50, 30,  0,   0,   0},
      {100, 100, 100, 50,  50,  50,  100, 100, 100, 90,  90,
       90,  200, 200, 200, 150, 150, 150, 50,  50,  50,  0,
       0,   0,   100, 50,  150, 200, 150, 50,  200, 200, 200}};
  EXPECT_EQ(purchaseTimestamps0, expectPurchaseTimestamps);

  auto future2 = std::async(
      revealTimestamps<0>,
      std::reference_wrapper<const std::vector<SecTimestamp<0>>>(
          publisherDeserialized_.purchaseTimestamps));
  auto future3 = std::async(
      revealTimestamps<1>,
      std::reference_wrapper<const std::vector<SecTimestamp<1>>>(
          partnerDeserialized_.purchaseTimestamps));
  auto deserializedPurchaseTimestamps = future2.get();
  future3.get();

  EXPECT_EQ(purchaseTimestamps0, deserializedPurchaseTimestamps);
}

TEST_P(InputProcessorTest, testThresholdTimestamps) {
  auto future0 = std::async(
      revealTimestamps<0>,
      std::reference_wrapper<const std::vector<SecTimestamp<0>>>(
          publisherInputProcessor_.getLiftGameProcessedData()
              .thresholdTimestamps));
  auto future1 = std::async(
      revealTimestamps<1>,
      std::reference_wrapper<const std::vector<SecTimestamp<1>>>(
          partnerInputProcessor_.getLiftGameProcessedData()
              .thresholdTimestamps));
  auto thresholdTimestamps0 = future0.get();
  future1.get();
  std::vector<std::vector<uint64_t>> expectThresholdTimestamps = {
      {0,  0,  0,  0,  0, 0, 0, 0, 0, 0, 0,   0,  160, 160, 160, 60, 60,
       60, 40, 40, 40, 0, 0, 0, 0, 0, 0, 160, 60, 40,  0,   0,   0},
      {110, 110, 110, 60,  60,  60,  110, 110, 110, 100, 100,
       100, 210, 210, 210, 160, 160, 160, 60,  60,  60,  0,
       0,   0,   110, 60,  160, 210, 160, 60,  210, 210, 210}};
  EXPECT_EQ(thresholdTimestamps0, expectThresholdTimestamps);

  auto future2 = std::async(
      revealTimestamps<0>,
      std::reference_wrapper<const std::vector<SecTimestamp<0>>>(
          publisherDeserialized_.thresholdTimestamps));
  auto future3 = std::async(
      revealTimestamps<1>,
      std::reference_wrapper<const std::vector<SecTimestamp<1>>>(
          partnerDeserialized_.thresholdTimestamps));
  auto deserializedThresholdTimestamps = future2.get();
  future3.get();

  EXPECT_EQ(thresholdTimestamps0, deserializedThresholdTimestamps);
}

TEST_P(InputProcessorTest, testAnyValidPurchaseTimestamp) {
  auto future0 = std::async([&] {
    return publisherInputProcessor_.getLiftGameProcessedData()
        .anyValidPurchaseTimestamp.openToParty(0)
        .getValue();
  });
  auto future1 = std::async([&] {
    return partnerInputProcessor_.getLiftGameProcessedData()
        .anyValidPurchaseTimestamp.openToParty(0)
        .getValue();
  });
  auto anyValidPurchaseTimestamp0 = future0.get();
  auto anyValidPurchaseTimestamp1 = future1.get();
  std::vector<bool> expectAnyValidPurchaseTimestamp = {
      1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
      1, 1, 1, 1, 0, 0, 0, 1, 1, 1, 1, 1, 1, 1, 1, 1};
  EXPECT_EQ(anyValidPurchaseTimestamp0, expectAnyValidPurchaseTimestamp);

  auto future2 = std::async([&] {
    return publisherDeserialized_.anyValidPurchaseTimestamp.openToParty(0)
        .getValue();
  });
  auto future3 = std::async([&] {
    return partnerDeserialized_.anyValidPurchaseTimestamp.openToParty(0)
        .getValue();
  });

  auto anyValidPurchaseTimestampDeserialized = future2.get();
  future3.get();

  EXPECT_EQ(anyValidPurchaseTimestamp0, anyValidPurchaseTimestampDeserialized);
}

template <int schedulerId>
std::vector<std::vector<int64_t>> revealValues(
    std::reference_wrapper<const std::vector<SecValue<schedulerId>>> values) {
  std::vector<std::vector<int64_t>> result;
  for (size_t i = 0; i < values.get().size(); ++i) {
    result.push_back(std::move(values.get().at(i).openToParty(0).getValue()));
  }
  return result;
}

TEST_P(InputProcessorTest, testPurchaseValues) {
  auto future0 = std::async(
      revealValues<0>,
      std::reference_wrapper<const std::vector<SecValue<0>>>(
          publisherInputProcessor_.getLiftGameProcessedData().purchaseValues));
  auto future1 = std::async(
      revealValues<1>,
      std::reference_wrapper<const std::vector<SecValue<1>>>(
          partnerInputProcessor_.getLiftGameProcessedData().purchaseValues));
  auto purchaseValues0 = future0.get();
  future1.get();
  std::vector<std::vector<int64_t>> expectPurchaseValues = {
      {0,  0,  0,  0,  0, 0, 0, 0, 0, 0, 0,  0,  10, 10, 10, 10, 10,
       10, 10, 10, 10, 0, 0, 0, 0, 0, 0, 10, 10, 10, 0,  0,  0},
      {0,  0,  0,  20, 20, 20, 20, 20, 20, 20, 20, 20, 20, 20,  20,  20, 20,
       20, 20, 20, 20, 0,  0,  0,  50, 50, 50, 20, 20, 20, -50, -50, -50}};
  EXPECT_EQ(purchaseValues0, expectPurchaseValues);

  auto future2 = std::async(
      revealValues<0>,
      std::reference_wrapper<const std::vector<SecValue<0>>>(
          publisherDeserialized_.purchaseValues));
  auto future3 = std::async(
      revealValues<1>,
      std::reference_wrapper<const std::vector<SecValue<1>>>(
          partnerDeserialized_.purchaseValues));
  auto deserializedPurchaseValues = future2.get();
  future3.get();

  EXPECT_EQ(purchaseValues0, deserializedPurchaseValues);
}

template <int schedulerId>
std::vector<std::vector<int64_t>> revealValueSquared(
    std::reference_wrapper<const std::vector<SecValueSquared<schedulerId>>>
        values) {
  std::vector<std::vector<int64_t>> result;
  for (size_t i = 0; i < values.get().size(); ++i) {
    result.push_back(std::move(values.get().at(i).openToParty(0).getValue()));
  }
  return result;
}

TEST_P(InputProcessorTest, testPurchaseValueSquared) {
  auto future0 = std::async(
      revealValueSquared<0>,
      std::reference_wrapper<const std::vector<SecValueSquared<0>>>(
          publisherInputProcessor_.getLiftGameProcessedData()
              .purchaseValueSquared));
  auto future1 = std::async(
      revealValueSquared<1>,
      std::reference_wrapper<const std::vector<SecValueSquared<1>>>(
          partnerInputProcessor_.getLiftGameProcessedData()
              .purchaseValueSquared));
  auto purchaseValueSquared0 = future0.get();
  future1.get();
  // squared sum of purchase value in each row
  std::vector<std::vector<int64_t>> expectPurchaseValueSquared = {
      {0,   0,   0,    400,  400,  400, 400, 400, 400,  400,  400,
       400, 900, 900,  900,  900,  900, 900, 900, 900,  900,  0,
       0,   0,   2500, 2500, 2500, 900, 900, 900, 2500, 2500, 2500},
      {0,   0,   0,    400,  400,  400, 400, 400, 400,  400,  400,
       400, 400, 400,  400,  400,  400, 400, 400, 400,  400,  0,
       0,   0,   2500, 2500, 2500, 400, 400, 400, 2500, 2500, 2500}};
  EXPECT_EQ(purchaseValueSquared0, expectPurchaseValueSquared);

  auto future2 = std::async(
      revealValueSquared<0>,
      std::reference_wrapper<const std::vector<SecValueSquared<0>>>(
          publisherDeserialized_.purchaseValueSquared));
  auto future3 = std::async(
      revealValueSquared<1>,
      std::reference_wrapper<const std::vector<SecValueSquared<1>>>(
          partnerDeserialized_.purchaseValueSquared));
  auto deserializedPurchaseValueSquared = future2.get();
  future3.get();

  EXPECT_EQ(purchaseValueSquared0, deserializedPurchaseValueSquared);
}

TEST_P(InputProcessorTest, testReach) {
  auto future0 = std::async([&] {
    return publisherInputProcessor_.getLiftGameProcessedData()
        .testReach.openToParty(0)
        .getValue();
  });
  auto future1 = std::async([&] {
    return partnerInputProcessor_.getLiftGameProcessedData()
        .testReach.openToParty(0)
        .getValue();
  });
  auto testReach0 = future0.get();
  future1.get();

  std::vector<bool> expectTestReach = {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                                       0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                                       0, 0, 1, 1, 1, 1, 1, 1, 0, 0, 0};
  EXPECT_EQ(testReach0, expectTestReach);

  auto future2 = std::async([&] {
    return publisherDeserialized_.testReach.openToParty(0).getValue();
  });
  auto future3 = std::async(
      [&] { return partnerDeserialized_.testReach.openToParty(0).getValue(); });

  auto testReachDeserialized = future2.get();
  future3.get();

  EXPECT_EQ(testReach0, testReachDeserialized);
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
