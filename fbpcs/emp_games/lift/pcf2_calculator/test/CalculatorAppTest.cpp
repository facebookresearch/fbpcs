/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <cmath>
#include <filesystem>
#include <memory>
#include <string>
#include <thread>
#include <unordered_map>

#include <gtest/gtest.h>
#include "folly/Random.h"

#include <fbpcf/io/api/FileIOWrappers.h>
#include "fbpcf/engine/communication/SocketPartyCommunicationAgentFactory.h"
#include "fbpcf/engine/communication/test/AgentFactoryCreationHelper.h"
#include "fbpcf/engine/communication/test/SocketInTestHelper.h"
#include "fbpcf/engine/communication/test/TlsCommunicationUtils.h"
#include "fbpcs/emp_games/common/Csv.h"
#include "fbpcs/emp_games/common/test/TestUtils.h"
#include "fbpcs/emp_games/lift/metadata_compaction/MetadataCompactorGameFactory.h"
#include "fbpcs/emp_games/lift/pcf2_calculator/CalculatorApp.h"
#include "fbpcs/emp_games/lift/pcf2_calculator/input_processing/InputProcessor.h"
#include "fbpcs/emp_games/lift/pcf2_calculator/sample_input/SampleInput.h"
#include "fbpcs/emp_games/lift/pcf2_calculator/test/common/GenFakeData.h"
#include "fbpcs/emp_games/lift/pcf2_calculator/test/common/LiftCalculator.h"

namespace private_lift {

template <int schedulerId>
void runCalculatorApp(
    int myId,
    const int numConversionsPerUser,
    const bool computePublisherBreakdowns,
    const int epoch,
    const std::string& inputPath,
    const std::string& inputGlobalParamsPath,
    const std::string& outputPath,
    bool useXorEncryption,
    std::unique_ptr<
        fbpcf::engine::communication::IPartyCommunicationAgentFactory>
        communicationAgentFactory) {
  auto metricCollector =
      std::make_shared<fbpcf::util::MetricCollector>("calculator_test");

  auto app = std::make_unique<CalculatorApp<schedulerId>>(
      myId,
      std::move(communicationAgentFactory),
      numConversionsPerUser,
      computePublisherBreakdowns,
      epoch,
      std::vector<std::string>{inputPath},
      inputGlobalParamsPath,
      std::vector<std::string>{outputPath},
      inputGlobalParamsPath != "",
      metricCollector,
      0,
      1,
      useXorEncryption);
  app->run();
}

template <int schedulerId>
void runUDPInputProcessorWithScheduler(
    int party,
    const std::string& inputPath,
    const std::string& globalParamsOutputPath,
    const std::string& secretSharesOutputPath,
    bool computePublisherBreakdowns,
    int epoch,
    int numConversionsPerUser,
    bool useXorEncryption,
    std::unique_ptr<
        fbpcf::engine::communication::IPartyCommunicationAgentFactory>
        communicationAgentFactory) {
  auto scheduler = useXorEncryption
      ? fbpcf::scheduler::getLazySchedulerFactoryWithRealEngine(
            party, *communicationAgentFactory)
            ->create()
      : fbpcf::scheduler::NetworkPlaintextSchedulerFactory<false>(
            party, *communicationAgentFactory)
            .create();

  InputData inputData(
      inputPath,
      InputData::LiftMPCType::Standard,
      computePublisherBreakdowns,
      epoch,
      numConversionsPerUser);

  auto compactorGameFactory = MetadataCompactorGameFactory<schedulerId>(
      std::move(communicationAgentFactory));

  auto compactorGame = compactorGameFactory.create(std::move(scheduler), party);
  auto inputProcessor = compactorGame->play(inputData, numConversionsPerUser);
  inputProcessor->getLiftGameProcessedData().writeToCSV(
      globalParamsOutputPath, secretSharesOutputPath);
}

class CalculatorAppTestFixture
    : public ::testing::TestWithParam<std::tuple<bool, bool, bool, bool>> {
 protected:
  std::string publisherPlaintextInputPath_;
  std::string partnerPlaintextInputPath_;
  std::string publisherSecretInputPath_;
  std::string partnerSecretInputPath_;
  std::string publisherGlobalParamsInputPath_;
  std::string partnerGlobalParamsInputPath_;
  std::string publisherOutputPath_;
  std::string partnerOutputPath_;
  std::string tlsDir_;
  int epoch_ = 1546300800;

  void SetUp() override {
    std::string tempDir = std::filesystem::temp_directory_path();
    publisherPlaintextInputPath_ = folly::sformat(
        "{}/publisher_plaintext_{}.csv",
        tempDir,
        folly::Random::secureRand64());
    partnerPlaintextInputPath_ = folly::sformat(
        "{}/partner_plaintext_{}.csv", tempDir, folly::Random::secureRand64());
    publisherSecretInputPath_ = folly::sformat(
        "{}/publisher_secret_{}.csv", tempDir, folly::Random::secureRand64());
    partnerSecretInputPath_ = folly::sformat(
        "{}/partner_secret_{}.csv", tempDir, folly::Random::secureRand64());
    publisherGlobalParamsInputPath_ = folly::sformat(
        "{}/publisher_global_params_{}.csv",
        tempDir,
        folly::Random::secureRand64());
    partnerGlobalParamsInputPath_ = folly::sformat(
        "{}/partner_global_params_{}.csv",
        tempDir,
        folly::Random::secureRand64());
    publisherOutputPath_ = folly::sformat(
        "{}/res_publisher_{}", tempDir, folly::Random::secureRand64());
    partnerOutputPath_ = folly::sformat(
        "{}/res_partner_{}", tempDir, folly::Random::secureRand64());
    tlsDir_ = fbpcf::engine::communication::setUpTlsFiles();
  }

  void TearDown() override {
    std::filesystem::remove(publisherPlaintextInputPath_);
    std::filesystem::remove(partnerPlaintextInputPath_);
    std::filesystem::remove(publisherSecretInputPath_);
    std::filesystem::remove(partnerSecretInputPath_);
    std::filesystem::remove(publisherGlobalParamsInputPath_);
    std::filesystem::remove(partnerGlobalParamsInputPath_);
    std::filesystem::remove(publisherOutputPath_);
    std::filesystem::remove(partnerOutputPath_);
    fbpcf::engine::communication::deleteTlsFiles(tlsDir_);
  }

  void setupUDPSecretShareInputs(
      const std::string& publisherInputPath,
      const std::string& partnerInputPath,
      const std::string& publisherOutputPath,
      const std::string& partnerOutputPath,
      const std::string& publisherGlobalParamsOutputPath,
      const std::string& partnerGlobalParamsOutputPath,
      int numConversionsPerUser,
      bool computePublisherBreakdowns,
      bool useTls,
      bool useXorEncryption) {
    fbpcf::engine::communication::SocketPartyCommunicationAgent::TlsInfo
        tlsInfo;
    tlsInfo.certPath = useTls ? (tlsDir_ + "/cert.pem") : "";
    tlsInfo.keyPath = useTls ? (tlsDir_ + "/key.pem") : "";
    tlsInfo.passphrasePath = useTls ? (tlsDir_ + "/passphrase.pem") : "";
    tlsInfo.rootCaCertPath = useTls ? (tlsDir_ + "/ca_cert.pem") : "";
    tlsInfo.useTls = useTls;

    auto [communicationAgentFactory0, communicationAgentFactory1] =
        fbpcf::engine::communication::getSocketAgentFactoryPair(tlsInfo);
    int epoch = 1546300800;

    auto future0 = std::async(
        runUDPInputProcessorWithScheduler<0>,
        0,
        publisherInputPath,
        publisherGlobalParamsOutputPath,
        publisherOutputPath,
        computePublisherBreakdowns,
        epoch_,
        numConversionsPerUser,
        useXorEncryption,
        std::move(communicationAgentFactory0));

    auto future1 = std::async(
        runUDPInputProcessorWithScheduler<1>,
        1,
        partnerInputPath,
        partnerGlobalParamsOutputPath,
        partnerOutputPath,
        computePublisherBreakdowns,
        epoch_,
        numConversionsPerUser,
        useXorEncryption,
        std::move(communicationAgentFactory1));

    future0.get();
    future1.get();
  }

  GroupedLiftMetrics runTest(
      const std::string& publisherInputPath,
      const std::string& partnerInputPath,
      const std::string& inputGlobalParamsPath,
      const std::string& publisherOutputPath,
      const std::string& partnerOutputPath,
      const int numConversionsPerUser,
      const bool computePublisherBreakdowns,
      bool useTls,
      bool useXorEncryption) {
    fbpcf::engine::communication::SocketPartyCommunicationAgent::TlsInfo
        tlsInfo;
    tlsInfo.certPath = useTls ? (tlsDir_ + "/cert.pem") : "";
    tlsInfo.keyPath = useTls ? (tlsDir_ + "/key.pem") : "";
    tlsInfo.passphrasePath = useTls ? (tlsDir_ + "/passphrase.pem") : "";
    tlsInfo.rootCaCertPath = useTls ? (tlsDir_ + "/ca_cert.pem") : "";
    tlsInfo.useTls = useTls;

    auto [communicationAgentFactoryAlice, communicationAgentFactoryBob] =
        fbpcf::engine::communication::getSocketAgentFactoryPair(tlsInfo);

    auto future0 = std::async(
        runCalculatorApp<0>,
        0,
        numConversionsPerUser,
        computePublisherBreakdowns,
        epoch_,
        publisherInputPath,
        inputGlobalParamsPath,
        publisherOutputPath,
        useXorEncryption,
        std::move(communicationAgentFactoryAlice));

    auto future1 = std::async(
        runCalculatorApp<1>,
        1,
        numConversionsPerUser,
        computePublisherBreakdowns,
        epoch_,
        partnerInputPath,
        inputGlobalParamsPath,
        partnerOutputPath,
        useXorEncryption,
        std::move(communicationAgentFactoryBob));

    future0.get();
    future1.get();
    auto publisherResult = GroupedLiftMetrics::fromJson(
        fbpcf::io::FileIOWrappers::readFile(publisherOutputPath));
    auto partnerResult = GroupedLiftMetrics::fromJson(
        fbpcf::io::FileIOWrappers::readFile(partnerOutputPath));

    return useXorEncryption ? publisherResult ^ partnerResult : publisherResult;
  }

  GroupedLiftMetrics runUdpTest(
      const std::string& publisherInputPath,
      const std::string& partnerInputPath,
      const std::string& publisherGlobalParamsPath,
      const std::string& partnerGlobalParamsPath,
      const std::string& publisherSecretSharesPath,
      const std::string& partnerSecretSharesPath,
      const std::string& publisherOutputPath,
      const std::string& partnerOutputPath,
      const int numConversionsPerUser,
      const bool computePublisherBreakdowns,
      bool useTls,
      bool useXorEncryption) {
    setupUDPSecretShareInputs(
        publisherInputPath,
        partnerInputPath,
        publisherSecretSharesPath,
        partnerSecretSharesPath,
        publisherGlobalParamsPath,
        partnerGlobalParamsPath,
        numConversionsPerUser,
        computePublisherBreakdowns,
        useTls,
        useXorEncryption);

    return runTest(
        publisherSecretSharesPath,
        partnerSecretSharesPath,
        publisherGlobalParamsPath,
        publisherOutputPath,
        partnerOutputPath,
        numConversionsPerUser,
        computePublisherBreakdowns,
        useTls,
        useXorEncryption);
  }
};

TEST_P(CalculatorAppTestFixture, TestCorrectness) {
  int numConversionsPerUser = 2;
  std::string publisherInputPath = sample_input::getPublisherInput3().native();
  std::string partnerInputPath = sample_input::getPartnerInput2().native();
  std::string expectedOutputPath =
      sample_input::getCorrectnessOutput().native();

  bool useTls = std::get<0>(GetParam());
  bool useXorEncryption = std::get<1>(GetParam());

  // test with and w/o computing publisher breakdowns
  bool computePublisherBreakdowns = std::get<2>(GetParam());
  bool readInputFromSecretShares = std::get<3>(GetParam());

  GroupedLiftMetrics result = readInputFromSecretShares
      ? runUdpTest(
            publisherInputPath,
            partnerInputPath,
            publisherGlobalParamsInputPath_,
            partnerGlobalParamsInputPath_,
            publisherSecretInputPath_,
            partnerSecretInputPath_,
            publisherOutputPath_,
            partnerOutputPath_,
            numConversionsPerUser,
            computePublisherBreakdowns,
            useTls,
            useXorEncryption)
      : runTest(
            publisherInputPath,
            partnerInputPath,
            "",
            publisherOutputPath_,
            partnerOutputPath_,
            numConversionsPerUser,
            computePublisherBreakdowns,
            useTls,
            useXorEncryption);

  auto expectedResult = GroupedLiftMetrics::fromJson(
      fbpcf::io::FileIOWrappers::readFile(expectedOutputPath));

  // No publisher breakdown computation required, remove the
  // breakdown data from the expected output before result validation
  if (!computePublisherBreakdowns) {
    expectedResult.publisherBreakdowns.clear();
  }

  EXPECT_EQ(expectedResult, result);
}

GroupedLiftMetrics computeCorrectResults(
    const std::string& publisherPlaintextInputPath,
    const std::string& partnerPlaintextInputPath,
    bool usingPublisherBreakdowns,
    bool usingCohorts) {
  // Calculate expected results with simple lift calculator
  LiftCalculator liftCalculator(
      usingCohorts ? 4 : 0, usingPublisherBreakdowns ? 2 : 0, 0);
  std::ifstream inFilePublisher{publisherPlaintextInputPath};
  std::ifstream inFilePartner{partnerPlaintextInputPath};
  int32_t tsOffset = 10;
  std::string linePublisher;
  std::string linePartner;
  getline(inFilePublisher, linePublisher);
  getline(inFilePartner, linePartner);
  auto headerPublisher =
      private_measurement::csv::splitByComma(linePublisher, false);
  auto headerPartner =
      private_measurement::csv::splitByComma(linePartner, false);
  std::unordered_map<std::string, int> colNameToIndex =
      liftCalculator.mapColToIndex(headerPublisher, headerPartner);
  GroupedLiftMetrics results = liftCalculator.compute(
      inFilePublisher, inFilePartner, colNameToIndex, tsOffset, false);

  if (!usingPublisherBreakdowns) {
    results.publisherBreakdowns.clear();
  }

  return results;
}

void generateSyntheticData(
    const std::string& publisherPlaintextInputPath,
    const std::string& partnerPlaintextInputPath,
    int numRows,
    int numConversionsPerUser,
    bool generatePublisherBreakdowns,
    bool useCohorts) {
  // Generate test input files with random data
  GenFakeData testDataGenerator;
  LiftFakeDataParams params;
  params.setNumRows(numRows)
      .setOpportunityRate(0.5)
      .setTestRate(0.5)
      .setPurchaseRate(0.5)
      .setIncrementalityRate(0.0)
      .setNumConversions(numConversionsPerUser)
      .setOmitValuesColumn(false)
      .setEpoch(1546300800);

  if (generatePublisherBreakdowns) {
    params.setNumBreakdowns(2);
  }

  if (useCohorts) {
    params.setNumCohorts(4);
  }

  testDataGenerator.genFakeInputFiles(
      publisherPlaintextInputPath, partnerPlaintextInputPath, params);
}

TEST_P(CalculatorAppTestFixture, TestCorrectnessRandomInput) {
  // Run calculator app with test input
  bool useTls = std::get<0>(GetParam());
  bool useXorEncryption = std::get<1>(GetParam());
  bool computePublisherBreakdowns = std::get<2>(GetParam());
  bool readInputFromSecretShares = std::get<3>(GetParam());

  // Generate test input files with random data
  int numConversionsPerUser = 25;
  generateSyntheticData(
      publisherPlaintextInputPath_,
      partnerPlaintextInputPath_,
      15,
      numConversionsPerUser,
      computePublisherBreakdowns,
      false);

  GroupedLiftMetrics res = readInputFromSecretShares
      ? runUdpTest(
            publisherPlaintextInputPath_,
            partnerPlaintextInputPath_,
            publisherGlobalParamsInputPath_,
            partnerGlobalParamsInputPath_,
            publisherSecretInputPath_,
            partnerSecretInputPath_,
            publisherOutputPath_,
            partnerOutputPath_,
            numConversionsPerUser,
            computePublisherBreakdowns,
            useTls,
            useXorEncryption)
      : runTest(
            publisherPlaintextInputPath_,
            partnerPlaintextInputPath_,
            "",
            publisherOutputPath_,
            partnerOutputPath_,
            numConversionsPerUser,
            computePublisherBreakdowns,
            useTls,
            useXorEncryption);

  GroupedLiftMetrics expectedResult = computeCorrectResults(
      publisherPlaintextInputPath_,
      partnerPlaintextInputPath_,
      computePublisherBreakdowns,
      false);

  EXPECT_EQ(expectedResult, res);
}

TEST_P(CalculatorAppTestFixture, TestCorrectnessRandomInputAndCohort) {
  // Run calculator app with test input
  bool useTls = std::get<0>(GetParam());
  bool useXorEncryption = std::get<1>(GetParam());
  bool computePublisherBreakdowns = std::get<2>(GetParam());
  bool readInputFromSecretShares = std::get<3>(GetParam());

  // Generate test input files with random data
  int numConversionsPerUser = 25;

  generateSyntheticData(
      publisherPlaintextInputPath_,
      partnerPlaintextInputPath_,
      15,
      numConversionsPerUser,
      computePublisherBreakdowns,
      true);

  GroupedLiftMetrics res = readInputFromSecretShares
      ? runUdpTest(
            publisherPlaintextInputPath_,
            partnerPlaintextInputPath_,
            publisherGlobalParamsInputPath_,
            partnerGlobalParamsInputPath_,
            publisherSecretInputPath_,
            partnerSecretInputPath_,
            publisherOutputPath_,
            partnerOutputPath_,
            numConversionsPerUser,
            computePublisherBreakdowns,
            useTls,
            useXorEncryption)
      : runTest(
            publisherPlaintextInputPath_,
            partnerPlaintextInputPath_,
            "",
            publisherOutputPath_,
            partnerOutputPath_,
            numConversionsPerUser,
            computePublisherBreakdowns,
            useTls,
            useXorEncryption);

  GroupedLiftMetrics expectedResult = computeCorrectResults(
      publisherPlaintextInputPath_,
      partnerPlaintextInputPath_,
      computePublisherBreakdowns,
      true);

  EXPECT_EQ(expectedResult, res);
}

TEST_P(CalculatorAppTestFixture, TestWithEmptyInput) {
  // Run calculator app with test input
  bool useTls = std::get<0>(GetParam());
  bool useXorEncryption = std::get<1>(GetParam());
  bool computePublisherBreakdowns = std::get<2>(GetParam());
  bool readInputFromSecretShares = std::get<3>(GetParam());

  // Generate test input files with random data
  int numConversionsPerUser = 25;

  generateSyntheticData(
      publisherPlaintextInputPath_,
      partnerPlaintextInputPath_,
      0,
      numConversionsPerUser,
      computePublisherBreakdowns,
      true);

  GroupedLiftMetrics res = readInputFromSecretShares
      ? runUdpTest(
            publisherPlaintextInputPath_,
            partnerPlaintextInputPath_,
            publisherGlobalParamsInputPath_,
            partnerGlobalParamsInputPath_,
            publisherSecretInputPath_,
            partnerSecretInputPath_,
            publisherOutputPath_,
            partnerOutputPath_,
            numConversionsPerUser,
            computePublisherBreakdowns,
            useTls,
            useXorEncryption)
      : runTest(
            publisherPlaintextInputPath_,
            partnerPlaintextInputPath_,
            "",
            publisherOutputPath_,
            partnerOutputPath_,
            numConversionsPerUser,
            computePublisherBreakdowns,
            useTls,
            useXorEncryption);

  XLOG(INFO, res.toJson());

  GroupedLiftMetrics expectedResult(
      LiftMetrics(
          0,
          0,
          0,
          0,
          0,
          0,
          0,
          0,
          0,
          0,
          0,
          0,
          0,
          0,
          std::vector<int64_t>(),
          std::vector<int64_t>()),
      std::vector<LiftMetrics>(),
      std::vector<LiftMetrics>());

  EXPECT_EQ(expectedResult, res);
}

INSTANTIATE_TEST_SUITE_P(
    CalculatorAppTest,
    CalculatorAppTestFixture,
    ::testing::Combine(
        ::testing::Bool(),
        ::testing::Bool(),
        ::testing::Bool(),
        ::testing::Bool()),
    [](const testing::TestParamInfo<CalculatorAppTestFixture::ParamType>&
           info) {
      std::string tls = std::get<0>(info.param) ? "True" : "False";
      std::string useXorEncryption = std::get<1>(info.param) ? "True" : "False";
      std::string computePublisherBreakdowns =
          std::get<2>(info.param) ? "True" : "False";
      std::string readInputFromSecretShares =
          std::get<3>(info.param) ? "True" : "False";
      std::string name = folly::sformat(
          "TLS_{}_XOR_{}_ComputePublisherBreakdowns_{}_ReadInputFromSecretShares_{}",
          tls,
          useXorEncryption,
          computePublisherBreakdowns,
          readInputFromSecretShares);

      return name;
    });

} // namespace private_lift
