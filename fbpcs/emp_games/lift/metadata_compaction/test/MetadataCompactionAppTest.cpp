/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <gtest/gtest.h>
#include <filesystem>
#include <memory>
#include "folly/Format.h"
#include "folly/Random.h"

#include "fbpcf/engine/communication/test/AgentFactoryCreationHelper.h"
#include "fbpcf/scheduler/ISchedulerFactory.h"

#include "fbpcs/emp_games/lift/metadata_compaction/DummyMetadataCompactorGameFactory.h"
#include "fbpcs/emp_games/lift/metadata_compaction/MetadataCompactorApp.h"

#include "fbpcs/emp_games/lift/pcf2_calculator/input_processing/SecretShareInputProcessor.h"
#include "fbpcs/emp_games/lift/pcf2_calculator/test/common/GenFakeData.h"

namespace private_lift {
template <int schedulerId>
void runMetadataCompactionApp(
    int myId,
    int numConversionsPerUser,
    bool computePublisherBreakdowns,
    int epoch,
    const std::string& inputPath,
    const std::string& outputGlobalParamsPath,
    const std::string& outputSecretSharesPath,
    bool useXorEncryption,
    std::unique_ptr<
        fbpcf::engine::communication::IPartyCommunicationAgentFactory>
        communicationAgentFactory,
    std::unique_ptr<IMetadataCompactorGameFactory<schedulerId>>
        metadataCompactorGameFactory) {
  auto app = std::make_unique<MetadataCompactorApp<schedulerId>>(
      myId,
      std::move(communicationAgentFactory),
      std::move(metadataCompactorGameFactory),
      numConversionsPerUser,
      computePublisherBreakdowns,
      epoch,
      inputPath,
      outputGlobalParamsPath,
      outputSecretSharesPath,
      useXorEncryption);

  app->run();
}

template <int schedulerId>
std::unique_ptr<IInputProcessor<schedulerId>> createInputProcessorWithScheduler(
    int myId,
    bool useXorEncryption,
    std::string globalParamsPath,
    std::string secretSharesPath,
    std::unique_ptr<
        fbpcf::engine::communication::IPartyCommunicationAgentFactory>
        communicationAgentFactory) {
  auto scheduler = useXorEncryption
      ? fbpcf::scheduler::getLazySchedulerFactoryWithRealEngine(
            myId, *communicationAgentFactory)
            ->create()
      : fbpcf::scheduler::NetworkPlaintextSchedulerFactory<false>(
            myId, *communicationAgentFactory)
            .create();

  fbpcf::scheduler::SchedulerKeeper<schedulerId>::setScheduler(
      std::move(scheduler));

  return std::make_unique<SecretShareInputProcessor<schedulerId>>(
      globalParamsPath, secretSharesPath);
}

class MetadataCompactionAppTestFixture
    : public ::testing::TestWithParam<std::tuple<bool, bool>> {
 protected:
  std::string publisherInputPath_;
  std::string partnerInputPath_;
  std::string publisherGlobalParamsOutputPath_;
  std::string publisherSecretSharesOutputPath_;
  std::string partnerGlobalParamsOutputPath_;
  std::string partnerSecretSharesOutputPath_;

  void SetUp() override {
    std::string tempDir = std::filesystem::temp_directory_path();
    publisherInputPath_ = folly::sformat(
        "{}/publisher_{}.csv", tempDir, folly::Random::secureRand64());
    partnerInputPath_ = folly::sformat(
        "{}/partner_{}.csv", tempDir, folly::Random::secureRand64());
    publisherGlobalParamsOutputPath_ = folly::sformat(
        "{}/publisher_global_params_output_{}",
        tempDir,
        folly::Random::secureRand64());
    publisherSecretSharesOutputPath_ = folly::sformat(
        "{}/publisher_secret_shares_output_{}",
        tempDir,
        folly::Random::secureRand64());
    partnerGlobalParamsOutputPath_ = folly::sformat(
        "{}/partner_global_params_output_{}",
        tempDir,
        folly::Random::secureRand64());
    partnerSecretSharesOutputPath_ = folly::sformat(
        "{}/partner_secret_shares_output_{}",
        tempDir,
        folly::Random::secureRand64());
  }

  void TearDown() override {
    std::filesystem::remove(publisherInputPath_);
    std::filesystem::remove(partnerInputPath_);
    std::filesystem::remove(publisherGlobalParamsOutputPath_);
    std::filesystem::remove(publisherSecretSharesOutputPath_);
    std::filesystem::remove(partnerGlobalParamsOutputPath_);
    std::filesystem::remove(partnerSecretSharesOutputPath_);
  }

  std::pair<
      std::unique_ptr<IInputProcessor<2>>,
      std::unique_ptr<IInputProcessor<3>>>
  runTest(
      const std::string& publisherInputPath,
      const std::string& partnerInputPath,
      const std::string& publisherGlobalParamsOutputPath,
      const std::string& publisherSecretSharesOutputPath,
      const std::string& partnerGlobalParamsOutputPath,
      const std::string& partnerSecretSharesOutputPath,
      int numConversionsPerUser,
      bool computePublisherBreakdowns,
      bool useXorEncryption,
      std::unique_ptr<IMetadataCompactorGameFactory<0>> publisherGameFactory,
      std::unique_ptr<IMetadataCompactorGameFactory<1>> partnerGameFactory) {
    auto factories = fbpcf::engine::communication::getInMemoryAgentFactory(2);

    int epoch = 1546300800;
    auto future0 = std::async(
        runMetadataCompactionApp<0>,
        0,
        numConversionsPerUser,
        computePublisherBreakdowns,
        epoch,
        publisherInputPath,
        publisherGlobalParamsOutputPath,
        publisherSecretSharesOutputPath,
        useXorEncryption,
        std::move(factories[0]),
        std::move(publisherGameFactory));

    auto future1 = std::async(
        runMetadataCompactionApp<1>,
        1,
        numConversionsPerUser,
        computePublisherBreakdowns,
        epoch,
        partnerInputPath,
        partnerGlobalParamsOutputPath,
        partnerSecretSharesOutputPath,
        useXorEncryption,
        std::move(factories[1]),
        std::move(partnerGameFactory));

    future0.get();
    future1.get();

    factories = fbpcf::engine::communication::getInMemoryAgentFactory(2);

    auto future2 = std::async(
        createInputProcessorWithScheduler<2>,
        0,
        useXorEncryption,
        publisherGlobalParamsOutputPath,
        publisherSecretSharesOutputPath,
        std::move(factories[0]));

    auto future3 = std::async(
        createInputProcessorWithScheduler<3>,
        1,
        useXorEncryption,
        partnerGlobalParamsOutputPath,
        partnerSecretSharesOutputPath,
        std::move(factories[1]));

    auto publisherResults = future2.get();
    auto partnerResults = future3.get();

    return std::make_pair<
        std::unique_ptr<IInputProcessor<2>>,
        std::unique_ptr<IInputProcessor<3>>>(
        std::move(publisherResults), std::move(partnerResults));
  }
};

TEST_P(
    MetadataCompactionAppTestFixture,
    TestRandomOutputWithDummyCompactorGame) {
  int numConversionsPerUser = 25;
  GenFakeData testDataGenerator;
  LiftFakeDataParams params;
  params.setNumRows(100)
      .setOpportunityRate(.5)
      .setTestRate(.5)
      .setPurchaseRate(.5)
      .setIncrementalityRate(0.0)
      .setEpoch(1546300800)
      .setNumConversions(numConversionsPerUser);
  testDataGenerator.genFakePublisherInputFile(publisherInputPath_, params);
  testDataGenerator.genFakePartnerInputFile(partnerInputPath_, params);

  bool useXorEncryption = std::get<0>(GetParam());
  bool computePublisherBreakdowns = std::get<1>(GetParam());

  auto res = runTest(
      publisherInputPath_,
      partnerInputPath_,
      publisherGlobalParamsOutputPath_,
      publisherSecretSharesOutputPath_,
      partnerGlobalParamsOutputPath_,
      partnerSecretSharesOutputPath_,
      numConversionsPerUser,
      computePublisherBreakdowns,
      useXorEncryption,
      std::make_unique<DummyMetadataCompactorGameFactory<0>>(),
      std::make_unique<DummyMetadataCompactorGameFactory<1>>());

  std::unique_ptr<IInputProcessor<2>> publisherResults =
      std::move(std::get<0>(res));

  std::unique_ptr<IInputProcessor<3>> partnerResults =
      std::move(std::get<1>(res));

  EXPECT_EQ(publisherResults->getLiftGameProcessedData().numRows, 100);
  EXPECT_EQ(partnerResults->getLiftGameProcessedData().numRows, 100);
}

INSTANTIATE_TEST_SUITE_P(
    MetadataCompactionAppTest,
    MetadataCompactionAppTestFixture,
    ::testing::Combine(::testing::Bool(), ::testing::Bool()),
    [](const testing::TestParamInfo<
        MetadataCompactionAppTestFixture::ParamType>& info) {
      std::string useXorEncryption = std::get<0>(info.param) ? "True" : "False";
      std::string computePublisherBreakdowns =
          std::get<1>(info.param) ? "True" : "False";

      std::string name = "UseXor_" + useXorEncryption +
          "_ComputePublisherBreakdowns_" + computePublisherBreakdowns;
      return name;
    });

} // namespace private_lift
