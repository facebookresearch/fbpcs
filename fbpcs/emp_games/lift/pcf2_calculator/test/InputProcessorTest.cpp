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
#include "fbpcs/emp_games/lift/pcf2_calculator/SecretShareInputProcessor.h"

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
  writeToCSV(inputProcessor.get(), globalParamsPath, secretSharesPath);

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
  SecretShareInputProcessor<0> publisherSecretInputProcessor_;
  SecretShareInputProcessor<1> partnerSecretInputProcessor_;

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

    auto future4 = std::async(
        [](const std::string& globalParamsPath,
           const std::string& secretSharesPath) {
          return SecretShareInputProcessor<0>(
              globalParamsPath, secretSharesPath);
        },
        publisherGlobalParamsOutput,
        publisherSecretSharesOutput);

    auto future5 = std::async(
        [](const std::string& globalParamsPath,
           const std::string& secretSharesPath) {
          return SecretShareInputProcessor<1>(
              globalParamsPath, secretSharesPath);
        },
        partnerGlobalParamsOutput,
        partnerSecretSharesOutput);

    publisherSecretInputProcessor_ = future4.get();
    partnerSecretInputProcessor_ = future5.get();

    cleanup(publisherGlobalParamsOutput);
    cleanup(publisherSecretSharesOutput);
    cleanup(partnerGlobalParamsOutput);
    cleanup(partnerSecretSharesOutput);
  }
};

template <int schedulerId>
void assertNumRows(
    const LiftGameProcessedData<schedulerId>& liftGameProcessedData) {
  EXPECT_EQ(liftGameProcessedData.numRows, 33);
}

TEST_P(InputProcessorTest, testNumRows) {
  assertNumRows(publisherInputProcessor_.getLiftGameProcessedData());
  assertNumRows(partnerInputProcessor_.getLiftGameProcessedData());
  assertNumRows(publisherSecretInputProcessor_.getLiftGameProcessedData());
  assertNumRows(partnerSecretInputProcessor_.getLiftGameProcessedData());
  assertNumRows(publisherDeserialized_);
  assertNumRows(partnerDeserialized_);
}

template <int schedulerId>
void assertValueBits(
    const LiftGameProcessedData<schedulerId>& liftGameProcessedData) {
  EXPECT_EQ(liftGameProcessedData.valueBits, 10);
  EXPECT_EQ(liftGameProcessedData.valueSquaredBits, 15);
}

TEST_P(InputProcessorTest, testBitsForValues) {
  assertValueBits(publisherInputProcessor_.getLiftGameProcessedData());
  assertValueBits(partnerInputProcessor_.getLiftGameProcessedData());
  assertValueBits(publisherSecretInputProcessor_.getLiftGameProcessedData());
  assertValueBits(partnerSecretInputProcessor_.getLiftGameProcessedData());
  assertValueBits(publisherDeserialized_);
  assertValueBits(partnerDeserialized_);
}

template <int schedulerId>
void assertPartnerCohorts(
    const LiftGameProcessedData<schedulerId>& liftGameProcessedData) {
  EXPECT_EQ(liftGameProcessedData.numPartnerCohorts, 3);
}

TEST_P(InputProcessorTest, testNumPartnerCohorts) {
  assertPartnerCohorts(publisherInputProcessor_.getLiftGameProcessedData());
  assertPartnerCohorts(partnerInputProcessor_.getLiftGameProcessedData());
  assertPartnerCohorts(
      publisherSecretInputProcessor_.getLiftGameProcessedData());
  assertPartnerCohorts(partnerSecretInputProcessor_.getLiftGameProcessedData());
  assertPartnerCohorts(publisherDeserialized_);
  assertPartnerCohorts(partnerDeserialized_);
}

template <int schedulerId>
void assertNumBreakdowns(
    const LiftGameProcessedData<schedulerId>& liftGameProcessedData,
    bool computePublisherBreakdowns) {
  if (computePublisherBreakdowns) {
    EXPECT_EQ(liftGameProcessedData.numPublisherBreakdowns, 2);
  } else {
    EXPECT_EQ(liftGameProcessedData.numPublisherBreakdowns, 0);
  }
}

TEST_P(InputProcessorTest, testNumBreakdowns) {
  assertNumBreakdowns(
      publisherInputProcessor_.getLiftGameProcessedData(),
      computePublisherBreakdowns_);
  assertNumBreakdowns(
      partnerInputProcessor_.getLiftGameProcessedData(),
      computePublisherBreakdowns_);
  assertNumBreakdowns(
      publisherSecretInputProcessor_.getLiftGameProcessedData(),
      computePublisherBreakdowns_);
  assertNumBreakdowns(
      partnerSecretInputProcessor_.getLiftGameProcessedData(),
      computePublisherBreakdowns_);
  assertNumBreakdowns(publisherDeserialized_, computePublisherBreakdowns_);
  assertNumBreakdowns(partnerDeserialized_, computePublisherBreakdowns_);
}

template <int schedulerId>
void assertNumGroups(
    const LiftGameProcessedData<schedulerId>& liftGameProcessedData,
    bool computePublisherBreakdowns) {
  if (computePublisherBreakdowns) {
    EXPECT_EQ(liftGameProcessedData.numGroups, 12);
  } else {
    EXPECT_EQ(liftGameProcessedData.numGroups, 6);
  }
}

TEST_P(InputProcessorTest, testNumGroups) {
  assertNumGroups(
      publisherInputProcessor_.getLiftGameProcessedData(),
      computePublisherBreakdowns_);
  assertNumGroups(
      partnerInputProcessor_.getLiftGameProcessedData(),
      computePublisherBreakdowns_);
  assertNumGroups(
      publisherSecretInputProcessor_.getLiftGameProcessedData(),
      computePublisherBreakdowns_);
  assertNumGroups(
      partnerSecretInputProcessor_.getLiftGameProcessedData(),
      computePublisherBreakdowns_);
  assertNumGroups(publisherDeserialized_, computePublisherBreakdowns_);
  assertNumGroups(partnerDeserialized_, computePublisherBreakdowns_);
}

template <int schedulerId>
void assertNumTestGroups(
    const LiftGameProcessedData<schedulerId>& liftGameProcessedData,
    bool computePublisherBreakdowns) {
  if (computePublisherBreakdowns) {
    EXPECT_EQ(liftGameProcessedData.numTestGroups, 7);
  } else {
    EXPECT_EQ(liftGameProcessedData.numTestGroups, 4);
  }
}

TEST_P(InputProcessorTest, testNumTestGroups) {
  assertNumTestGroups(
      publisherInputProcessor_.getLiftGameProcessedData(),
      computePublisherBreakdowns_);
  assertNumTestGroups(
      partnerInputProcessor_.getLiftGameProcessedData(),
      computePublisherBreakdowns_);
  assertNumTestGroups(
      publisherSecretInputProcessor_.getLiftGameProcessedData(),
      computePublisherBreakdowns_);
  assertNumTestGroups(
      partnerSecretInputProcessor_.getLiftGameProcessedData(),
      computePublisherBreakdowns_);
  assertNumTestGroups(publisherDeserialized_, computePublisherBreakdowns_);
  assertNumTestGroups(partnerDeserialized_, computePublisherBreakdowns_);
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

  auto deserializedGroupIds =
      convertIndexSharesToGroupIds(publisherDeserialized_.indexShares);
  EXPECT_EQ(groupIds, deserializedGroupIds);

  deserializedGroupIds = convertIndexSharesToGroupIds(
      publisherSecretInputProcessor_.getLiftGameProcessedData().indexShares);

  EXPECT_EQ(deserializedGroupIds, groupIds);
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

  auto deserializedTestGroupIds =
      convertIndexSharesToGroupIds(publisherDeserialized_.testIndexShares);
  EXPECT_EQ(testGroupIds, deserializedTestGroupIds);

  deserializedTestGroupIds = convertIndexSharesToGroupIds(
      publisherSecretInputProcessor_.getLiftGameProcessedData()
          .testIndexShares);

  EXPECT_EQ(deserializedTestGroupIds, testGroupIds);
}

void assertOpportunityTimestamps(
    const LiftGameProcessedData<0>& publisherData,
    const LiftGameProcessedData<1>& partnerData) {
  auto future0 = std::async([&] {
    return publisherData.opportunityTimestamps.openToParty(0).getValue();
  });
  auto future1 = std::async([&] {
    return partnerData.opportunityTimestamps.openToParty(0).getValue();
  });

  auto opportunityTimestamps = future0.get();
  future1.get();

  std::vector<uint64_t> expectOpportunityTimestamps = {
      0,   0,   0,   100, 100, 100, 100, 100, 100, 100, 100,
      100, 100, 100, 100, 100, 100, 100, 100, 100, 100, 100,
      100, 100, 0,   100, 100, 100, 100, 100, 100, 100, 100};
  EXPECT_EQ(opportunityTimestamps, expectOpportunityTimestamps);
}

TEST_P(InputProcessorTest, testOpportunityTimestamps) {
  assertOpportunityTimestamps(
      publisherInputProcessor_.getLiftGameProcessedData(),
      partnerInputProcessor_.getLiftGameProcessedData());
  assertOpportunityTimestamps(publisherDeserialized_, partnerDeserialized_);
  assertOpportunityTimestamps(
      publisherSecretInputProcessor_.getLiftGameProcessedData(),
      partnerSecretInputProcessor_.getLiftGameProcessedData());
}

void assertIsValidOpportunityTimestamps(
    const LiftGameProcessedData<0>& publisherData,
    const LiftGameProcessedData<1>& partnerData) {
  auto future0 = std::async([&] {
    return publisherData.isValidOpportunityTimestamp.openToParty(0).getValue();
  });
  auto future1 = std::async([&] {
    return partnerData.isValidOpportunityTimestamp.openToParty(0).getValue();
  });

  auto isValidOpportunityTimestamp = future0.get();
  future1.get();

  std::vector<bool> expectIsValidOpportunityTimestamp = {
      0, 0, 0, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1,
      1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 1, 1, 1, 0, 1, 1};
  EXPECT_EQ(isValidOpportunityTimestamp, expectIsValidOpportunityTimestamp);
}

TEST_P(InputProcessorTest, testIsValidOpportunityTimestamp) {
  assertOpportunityTimestamps(
      publisherInputProcessor_.getLiftGameProcessedData(),
      partnerInputProcessor_.getLiftGameProcessedData());
  assertOpportunityTimestamps(publisherDeserialized_, partnerDeserialized_);
  assertOpportunityTimestamps(
      publisherSecretInputProcessor_.getLiftGameProcessedData(),
      partnerSecretInputProcessor_.getLiftGameProcessedData());
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

void assertPurchaseTimestamps(
    const LiftGameProcessedData<0>& publisherData,
    const LiftGameProcessedData<1>& partnerData) {
  auto future0 = std::async(
      revealTimestamps<0>,
      std::reference_wrapper<const std::vector<SecTimestamp<0>>>(
          publisherData.purchaseTimestamps));
  auto future1 = std::async(
      revealTimestamps<1>,
      std::reference_wrapper<const std::vector<SecTimestamp<1>>>(
          partnerData.purchaseTimestamps));
  auto purchaseTimestamps = future0.get();
  future1.get();
  std::vector<std::vector<uint64_t>> expectPurchaseTimestamps = {
      {0,  0,  0,  0,  0, 0, 0, 0, 0, 0, 0,   0,  150, 150, 150, 50, 50,
       50, 30, 30, 30, 0, 0, 0, 0, 0, 0, 150, 50, 30,  0,   0,   0},
      {100, 100, 100, 50,  50,  50,  100, 100, 100, 90,  90,
       90,  200, 200, 200, 150, 150, 150, 50,  50,  50,  0,
       0,   0,   100, 50,  150, 200, 150, 50,  200, 200, 200}};
  EXPECT_EQ(purchaseTimestamps, expectPurchaseTimestamps);
}

TEST_P(InputProcessorTest, testPurchaseTimestamps) {
  assertPurchaseTimestamps(
      publisherInputProcessor_.getLiftGameProcessedData(),
      partnerInputProcessor_.getLiftGameProcessedData());
  assertPurchaseTimestamps(publisherDeserialized_, partnerDeserialized_);
  assertPurchaseTimestamps(
      publisherSecretInputProcessor_.getLiftGameProcessedData(),
      partnerSecretInputProcessor_.getLiftGameProcessedData());
}

void assertThresholdTimestamps(
    const LiftGameProcessedData<0>& publisherData,
    const LiftGameProcessedData<1>& partnerData) {
  auto future0 = std::async(
      revealTimestamps<0>,
      std::reference_wrapper<const std::vector<SecTimestamp<0>>>(
          publisherData.thresholdTimestamps));
  auto future1 = std::async(
      revealTimestamps<1>,
      std::reference_wrapper<const std::vector<SecTimestamp<1>>>(
          partnerData.thresholdTimestamps));
  auto thresholdTimestamps = future0.get();
  future1.get();
  std::vector<std::vector<uint64_t>> expectThresholdTimestamps = {
      {0,  0,  0,  0,  0, 0, 0, 0, 0, 0, 0,   0,  160, 160, 160, 60, 60,
       60, 40, 40, 40, 0, 0, 0, 0, 0, 0, 160, 60, 40,  0,   0,   0},
      {110, 110, 110, 60,  60,  60,  110, 110, 110, 100, 100,
       100, 210, 210, 210, 160, 160, 160, 60,  60,  60,  0,
       0,   0,   110, 60,  160, 210, 160, 60,  210, 210, 210}};
  EXPECT_EQ(thresholdTimestamps, expectThresholdTimestamps);
}

TEST_P(InputProcessorTest, testThresholdTimestamps) {
  assertThresholdTimestamps(
      publisherInputProcessor_.getLiftGameProcessedData(),
      partnerInputProcessor_.getLiftGameProcessedData());
  assertThresholdTimestamps(publisherDeserialized_, partnerDeserialized_);
  assertThresholdTimestamps(
      publisherSecretInputProcessor_.getLiftGameProcessedData(),
      partnerSecretInputProcessor_.getLiftGameProcessedData());
}

void assertAnyValidPurchaseTimestamp(
    const LiftGameProcessedData<0>& publisherData,
    const LiftGameProcessedData<1>& partnerData) {
  auto future0 = std::async([&] {
    return publisherData.anyValidPurchaseTimestamp.openToParty(0).getValue();
  });
  auto future1 = std::async([&] {
    return partnerData.anyValidPurchaseTimestamp.openToParty(0).getValue();
  });
  auto anyValidPurchaseTimestamp = future0.get();
  future1.get();
  std::vector<bool> expectAnyValidPurchaseTimestamp = {
      1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
      1, 1, 1, 1, 0, 0, 0, 1, 1, 1, 1, 1, 1, 1, 1, 1};
  EXPECT_EQ(anyValidPurchaseTimestamp, expectAnyValidPurchaseTimestamp);
}

TEST_P(InputProcessorTest, testAnyValidPurchaseTimestamp) {
  assertAnyValidPurchaseTimestamp(
      publisherInputProcessor_.getLiftGameProcessedData(),
      partnerInputProcessor_.getLiftGameProcessedData());
  assertAnyValidPurchaseTimestamp(publisherDeserialized_, partnerDeserialized_);
  assertAnyValidPurchaseTimestamp(
      publisherSecretInputProcessor_.getLiftGameProcessedData(),
      partnerSecretInputProcessor_.getLiftGameProcessedData());
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

void assertPurchaseValues(
    const LiftGameProcessedData<0>& publisherData,
    const LiftGameProcessedData<1>& partnerData) {
  auto future0 = std::async(
      revealValues<0>,
      std::reference_wrapper<const std::vector<SecValue<0>>>(
          publisherData.purchaseValues));
  auto future1 = std::async(
      revealValues<1>,
      std::reference_wrapper<const std::vector<SecValue<1>>>(
          partnerData.purchaseValues));
  auto purchaseValues = future0.get();
  future1.get();
  std::vector<std::vector<int64_t>> expectPurchaseValues = {
      {0,  0,  0,  0,  0, 0, 0, 0, 0, 0, 0,  0,  10, 10, 10, 10, 10,
       10, 10, 10, 10, 0, 0, 0, 0, 0, 0, 10, 10, 10, 0,  0,  0},
      {0,  0,  0,  20, 20, 20, 20, 20, 20, 20, 20, 20, 20, 20,  20,  20, 20,
       20, 20, 20, 20, 0,  0,  0,  50, 50, 50, 20, 20, 20, -50, -50, -50}};
  EXPECT_EQ(purchaseValues, expectPurchaseValues);
}

TEST_P(InputProcessorTest, testPurchaseValues) {
  assertPurchaseValues(
      publisherInputProcessor_.getLiftGameProcessedData(),
      partnerInputProcessor_.getLiftGameProcessedData());
  assertPurchaseValues(publisherDeserialized_, partnerDeserialized_);
  assertPurchaseValues(
      publisherSecretInputProcessor_.getLiftGameProcessedData(),
      partnerSecretInputProcessor_.getLiftGameProcessedData());
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

void assertPurchaseValuesSquared(
    const LiftGameProcessedData<0>& publisherData,
    const LiftGameProcessedData<1>& partnerData) {
  auto future0 = std::async(
      revealValueSquared<0>,
      std::reference_wrapper<const std::vector<SecValueSquared<0>>>(
          publisherData.purchaseValueSquared));
  auto future1 = std::async(
      revealValueSquared<1>,
      std::reference_wrapper<const std::vector<SecValueSquared<1>>>(
          partnerData.purchaseValueSquared));
  auto purchaseValueSquared = future0.get();
  future1.get();
  // squared sum of purchase value in each row
  std::vector<std::vector<int64_t>> expectPurchaseValueSquared = {
      {0,   0,   0,    400,  400,  400, 400, 400, 400,  400,  400,
       400, 900, 900,  900,  900,  900, 900, 900, 900,  900,  0,
       0,   0,   2500, 2500, 2500, 900, 900, 900, 2500, 2500, 2500},
      {0,   0,   0,    400,  400,  400, 400, 400, 400,  400,  400,
       400, 400, 400,  400,  400,  400, 400, 400, 400,  400,  0,
       0,   0,   2500, 2500, 2500, 400, 400, 400, 2500, 2500, 2500}};
  EXPECT_EQ(purchaseValueSquared, expectPurchaseValueSquared);
}

TEST_P(InputProcessorTest, testPurchaseValueSquared) {
  assertPurchaseValuesSquared(
      publisherInputProcessor_.getLiftGameProcessedData(),
      partnerInputProcessor_.getLiftGameProcessedData());
  assertPurchaseValuesSquared(publisherDeserialized_, partnerDeserialized_);
  assertPurchaseValuesSquared(
      publisherSecretInputProcessor_.getLiftGameProcessedData(),
      partnerSecretInputProcessor_.getLiftGameProcessedData());
}

void assertReach(
    const LiftGameProcessedData<0>& publisherData,
    const LiftGameProcessedData<1>& partnerData) {
  auto future0 = std::async(
      [&] { return publisherData.testReach.openToParty(0).getValue(); });
  auto future1 = std::async(
      [&] { return partnerData.testReach.openToParty(0).getValue(); });
  auto testReach = future0.get();
  future1.get();

  std::vector<bool> expectTestReach = {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                                       0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                                       0, 0, 1, 1, 1, 1, 1, 1, 0, 0, 0};
  EXPECT_EQ(testReach, expectTestReach);
}

TEST_P(InputProcessorTest, testReach) {
  assertReach(
      publisherInputProcessor_.getLiftGameProcessedData(),
      partnerInputProcessor_.getLiftGameProcessedData());
  assertReach(publisherDeserialized_, partnerDeserialized_);
  assertReach(
      publisherSecretInputProcessor_.getLiftGameProcessedData(),
      partnerSecretInputProcessor_.getLiftGameProcessedData());
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
