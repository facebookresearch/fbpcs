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
#include "fbpcs/emp_games/lift/pcf2_calculator/input_processing/InputProcessor.h"
#include "fbpcs/emp_games/lift/pcf2_calculator/input_processing/SecretShareInputProcessor.h"
#include "fbpcs/emp_games/lift/pcf2_calculator/input_processing/test/TestUtil.h"

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
        baseDir + "../../sample_input/publisher_unittest3.csv";
    std::string partnerInputFilename =
        baseDir + "../../sample_input/partner_2_convs_unittest.csv";

    std::string publisherGlobalParamsOutput = folly::sformat(
        "{}../../sample_input/publisher_global_params_{}.json",
        baseDir,
        folly::Random::secureRand64());

    std::string publisherSecretSharesOutput = folly::sformat(
        "{}../../sample_input/publisher_secret_shares_{}.json",
        baseDir,
        folly::Random::secureRand64());

    std::string partnerGlobalParamsOutput = folly::sformat(
        "{}../../sample_input/partner_global_params_{}.json",
        baseDir,
        folly::Random::secureRand64());

    std::string partnerSecretSharesOutput = folly::sformat(
        "{}../../sample_input/partner_secret_shares_{}.json",
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

TEST_P(InputProcessorTest, testNumRows) {
  util::assertNumRows(publisherInputProcessor_.getLiftGameProcessedData());
  util::assertNumRows(partnerInputProcessor_.getLiftGameProcessedData());
  util::assertNumRows(
      publisherSecretInputProcessor_.getLiftGameProcessedData());
  util::assertNumRows(partnerSecretInputProcessor_.getLiftGameProcessedData());
  util::assertNumRows(publisherDeserialized_);
  util::assertNumRows(partnerDeserialized_);
}

TEST_P(InputProcessorTest, testBitsForValues) {
  util::assertValueBits(publisherInputProcessor_.getLiftGameProcessedData());
  util::assertValueBits(partnerInputProcessor_.getLiftGameProcessedData());
  util::assertValueBits(
      publisherSecretInputProcessor_.getLiftGameProcessedData());
  util::assertValueBits(
      partnerSecretInputProcessor_.getLiftGameProcessedData());
  util::assertValueBits(publisherDeserialized_);
  util::assertValueBits(partnerDeserialized_);
}

TEST_P(InputProcessorTest, testNumPartnerCohorts) {
  util::assertPartnerCohorts(
      publisherInputProcessor_.getLiftGameProcessedData());
  util::assertPartnerCohorts(partnerInputProcessor_.getLiftGameProcessedData());
  util::assertPartnerCohorts(
      publisherSecretInputProcessor_.getLiftGameProcessedData());
  util::assertPartnerCohorts(
      partnerSecretInputProcessor_.getLiftGameProcessedData());
  util::assertPartnerCohorts(publisherDeserialized_);
  util::assertPartnerCohorts(partnerDeserialized_);
}

TEST_P(InputProcessorTest, testNumBreakdowns) {
  util::assertNumBreakdowns(
      publisherInputProcessor_.getLiftGameProcessedData(),
      computePublisherBreakdowns_);
  util::assertNumBreakdowns(
      partnerInputProcessor_.getLiftGameProcessedData(),
      computePublisherBreakdowns_);
  util::assertNumBreakdowns(
      publisherSecretInputProcessor_.getLiftGameProcessedData(),
      computePublisherBreakdowns_);
  util::assertNumBreakdowns(
      partnerSecretInputProcessor_.getLiftGameProcessedData(),
      computePublisherBreakdowns_);
  util::assertNumBreakdowns(
      publisherDeserialized_, computePublisherBreakdowns_);
  util::assertNumBreakdowns(partnerDeserialized_, computePublisherBreakdowns_);
}

TEST_P(InputProcessorTest, testNumGroups) {
  util::assertNumGroups(
      publisherInputProcessor_.getLiftGameProcessedData(),
      computePublisherBreakdowns_);
  util::assertNumGroups(
      partnerInputProcessor_.getLiftGameProcessedData(),
      computePublisherBreakdowns_);
  util::assertNumGroups(
      publisherSecretInputProcessor_.getLiftGameProcessedData(),
      computePublisherBreakdowns_);
  util::assertNumGroups(
      partnerSecretInputProcessor_.getLiftGameProcessedData(),
      computePublisherBreakdowns_);
  util::assertNumGroups(publisherDeserialized_, computePublisherBreakdowns_);
  util::assertNumGroups(partnerDeserialized_, computePublisherBreakdowns_);
}

TEST_P(InputProcessorTest, testNumTestGroups) {
  util::assertNumTestGroups(
      publisherInputProcessor_.getLiftGameProcessedData(),
      computePublisherBreakdowns_);
  util::assertNumTestGroups(
      partnerInputProcessor_.getLiftGameProcessedData(),
      computePublisherBreakdowns_);
  util::assertNumTestGroups(
      publisherSecretInputProcessor_.getLiftGameProcessedData(),
      computePublisherBreakdowns_);
  util::assertNumTestGroups(
      partnerSecretInputProcessor_.getLiftGameProcessedData(),
      computePublisherBreakdowns_);
  util::assertNumTestGroups(
      publisherDeserialized_, computePublisherBreakdowns_);
  util::assertNumTestGroups(partnerDeserialized_, computePublisherBreakdowns_);
}

TEST_P(InputProcessorTest, testIndexShares) {
  util::assertIndexShares(
      publisherInputProcessor_.getLiftGameProcessedData(),
      computePublisherBreakdowns_);
  util::assertIndexShares(publisherDeserialized_, computePublisherBreakdowns_);
  util::assertIndexShares(
      publisherSecretInputProcessor_.getLiftGameProcessedData(),
      computePublisherBreakdowns_);
}

TEST_P(InputProcessorTest, testTestIndexShares) {
  util::assertTestIndexShares(
      publisherInputProcessor_.getLiftGameProcessedData(),
      computePublisherBreakdowns_);
  util::assertTestIndexShares(
      publisherDeserialized_, computePublisherBreakdowns_);
  util::assertTestIndexShares(
      publisherSecretInputProcessor_.getLiftGameProcessedData(),
      computePublisherBreakdowns_);
}

TEST_P(InputProcessorTest, testOpportunityTimestamps) {
  util::assertOpportunityTimestamps(
      publisherInputProcessor_.getLiftGameProcessedData(),
      partnerInputProcessor_.getLiftGameProcessedData());
  util::assertOpportunityTimestamps(
      publisherDeserialized_, partnerDeserialized_);
  util::assertOpportunityTimestamps(
      publisherSecretInputProcessor_.getLiftGameProcessedData(),
      partnerSecretInputProcessor_.getLiftGameProcessedData());
}

TEST_P(InputProcessorTest, testIsValidOpportunityTimestamp) {
  util::assertOpportunityTimestamps(
      publisherInputProcessor_.getLiftGameProcessedData(),
      partnerInputProcessor_.getLiftGameProcessedData());
  util::assertOpportunityTimestamps(
      publisherDeserialized_, partnerDeserialized_);
  util::assertOpportunityTimestamps(
      publisherSecretInputProcessor_.getLiftGameProcessedData(),
      partnerSecretInputProcessor_.getLiftGameProcessedData());
}

TEST_P(InputProcessorTest, testPurchaseTimestamps) {
  util::assertPurchaseTimestamps(
      publisherInputProcessor_.getLiftGameProcessedData(),
      partnerInputProcessor_.getLiftGameProcessedData());
  util::assertPurchaseTimestamps(publisherDeserialized_, partnerDeserialized_);
  util::assertPurchaseTimestamps(
      publisherSecretInputProcessor_.getLiftGameProcessedData(),
      partnerSecretInputProcessor_.getLiftGameProcessedData());
}

TEST_P(InputProcessorTest, testThresholdTimestamps) {
  util::assertThresholdTimestamps(
      publisherInputProcessor_.getLiftGameProcessedData(),
      partnerInputProcessor_.getLiftGameProcessedData());
  util::assertThresholdTimestamps(publisherDeserialized_, partnerDeserialized_);
  util::assertThresholdTimestamps(
      publisherSecretInputProcessor_.getLiftGameProcessedData(),
      partnerSecretInputProcessor_.getLiftGameProcessedData());
}

TEST_P(InputProcessorTest, testAnyValidPurchaseTimestamp) {
  util::assertAnyValidPurchaseTimestamp(
      publisherInputProcessor_.getLiftGameProcessedData(),
      partnerInputProcessor_.getLiftGameProcessedData());
  util::assertAnyValidPurchaseTimestamp(
      publisherDeserialized_, partnerDeserialized_);
  util::assertAnyValidPurchaseTimestamp(
      publisherSecretInputProcessor_.getLiftGameProcessedData(),
      partnerSecretInputProcessor_.getLiftGameProcessedData());
}

TEST_P(InputProcessorTest, testPurchaseValues) {
  util::assertPurchaseValues(
      publisherInputProcessor_.getLiftGameProcessedData(),
      partnerInputProcessor_.getLiftGameProcessedData());
  util::assertPurchaseValues(publisherDeserialized_, partnerDeserialized_);
  util::assertPurchaseValues(
      publisherSecretInputProcessor_.getLiftGameProcessedData(),
      partnerSecretInputProcessor_.getLiftGameProcessedData());
}

TEST_P(InputProcessorTest, testPurchaseValueSquared) {
  util::assertPurchaseValuesSquared(
      publisherInputProcessor_.getLiftGameProcessedData(),
      partnerInputProcessor_.getLiftGameProcessedData());
  util::assertPurchaseValuesSquared(
      publisherDeserialized_, partnerDeserialized_);
  util::assertPurchaseValuesSquared(
      publisherSecretInputProcessor_.getLiftGameProcessedData(),
      partnerSecretInputProcessor_.getLiftGameProcessedData());
}

TEST_P(InputProcessorTest, testReach) {
  util::assertReach(
      publisherInputProcessor_.getLiftGameProcessedData(),
      partnerInputProcessor_.getLiftGameProcessedData());
  util::assertReach(publisherDeserialized_, partnerDeserialized_);
  util::assertReach(
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
