/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <fbpcf/io/api/FileIOWrappers.h>
#include <filesystem>
#include <string>

#include <gtest/gtest.h>
#include "folly/Format.h"
#include "folly/Random.h"
#include "folly/logging/xlog.h"

#include "fbpcf/engine/communication/SocketPartyCommunicationAgentFactory.h"
#include "fbpcf/engine/communication/test/AgentFactoryCreationHelper.h"
#include "fbpcf/engine/communication/test/SocketInTestHelper.h"
#include "fbpcf/engine/communication/test/TlsCommunicationUtils.h"

#include "fbpcs/emp_games/common/TestUtil.h"
#include "fbpcs/emp_games/dotproduct/DotproductApp.h"
#include "fbpcs/emp_games/dotproduct/test/DotproductTestUtils.h"

namespace pcf2_dotproduct {

template <int PARTY, int schedulerId>
static void runGame(
    std::string serverIp,
    int port,
    std::string inputFilePath,
    std::string outputFilePath,
    bool useTls,
    std::unique_ptr<
        fbpcf::engine::communication::IPartyCommunicationAgentFactory>
        communicationAgentFactory) {
  const bool debugMode = false;
  int numFeatures = 50;
  int labelWidth = 16;

  auto metricCollector =
      std::make_shared<fbpcf::util::MetricCollector>("dotproduct_test");

  auto app = std::make_unique<pcf2_dotproduct::DotproductApp<PARTY, PARTY>>(
      std::move(communicationAgentFactory),
      inputFilePath,
      outputFilePath,
      numFeatures,
      labelWidth,
      metricCollector,
      debugMode);

  app->run();
}

// helper function for executing MPC game and verifying corresponding output
inline void testCorrectnessDotProductAppHelper(
    std::string serverIp,
    int port,
    std::vector<std::string> inputPathsAlice,
    std::vector<std::string> outputPathsAlice,
    std::vector<std::string> inputPathsBob,
    std::vector<std::string> outputPathsBob,
    std::vector<std::string> expectedOutputPaths,
    bool useTls,
    std::string& tlsDir) {
  fbpcf::engine::communication::SocketPartyCommunicationAgent::TlsInfo tlsInfo;
  tlsInfo.certPath = useTls ? (tlsDir + "/cert.pem") : "";
  tlsInfo.keyPath = useTls ? (tlsDir + "/key.pem") : "";
  tlsInfo.passphrasePath = useTls ? (tlsDir + "/passphrase.pem") : "";
  tlsInfo.rootCaCertPath = useTls ? (tlsDir + "/ca_cert.pem") : "";
  tlsInfo.useTls = useTls;

  auto [communicationAgentFactoryAlice, communicationAgentFactoryBob] =
      fbpcf::engine::communication::getSocketAgentFactoryPair(tlsInfo);
  for (int i = 0; i < inputPathsAlice.size(); i++) {
    auto futureAlice = std::async(
        runGame<0, 0>,
        "",
        port,
        inputPathsAlice.at(i),
        outputPathsAlice.at(i),
        useTls,
        std::move(communicationAgentFactoryAlice));
    auto futureBob = std::async(
        runGame<1, 1>,
        serverIp,
        port,
        inputPathsBob.at(i),
        outputPathsBob.at(i),
        useTls,
        std::move(communicationAgentFactoryBob));
    futureAlice.wait();
    futureBob.wait();

    // Read result and expected result
    auto result = parseResult(outputPathsAlice.at(i));
    auto expectedResult = parseResult(expectedOutputPaths.at(i));

    // Check that size of the result matches the expected size
    EXPECT_EQ(result.size(), expectedResult.size());

    // Check that values are equal
    bool equal = verifyOutput(result, expectedResult);
    EXPECT_TRUE(equal);
  }
}

TEST(DotproductAppTest, DotproductAppCorrectnessTest) {
  bool useTls = true;
  std::string tlsDir = fbpcf::engine::communication::setUpTlsFiles();
  int port =
      fbpcf::engine::communication::SocketInTestHelper::findNextOpenPort(5000);
  std::string baseDir =
      private_measurement::test_util::getBaseDirFromPath(__FILE__);
  std::string serverIp = "127.0.0.1";

  std::vector<std::string> inputFilenamesAlice;
  std::vector<std::string> inputFilenamesBob;
  std::vector<std::string> outputFilenamesAlice;
  std::vector<std::string> outputFilenamesBob;
  std::vector<std::string> expectedOutputFilenames;

  int numTestFiles = 1;
  std::string filePrefix = baseDir + "test_correctness/";

  for (size_t i = 0; i < numTestFiles; i++) {
    inputFilenamesAlice.push_back(
        folly::sformat("{}/publisher_dotprodtest_{}.csv", filePrefix, i));
    inputFilenamesBob.push_back(
        (folly::sformat("{}/partner_dotprodtest_{}.csv", filePrefix, i)));
    outputFilenamesAlice.push_back(
        folly::sformat("{}/outpub_dotprodtest_{}.csv", filePrefix, i));
    outputFilenamesBob.push_back(
        folly::sformat("{}/outpart_dotprodtest_{}.csv", filePrefix, i));
    expectedOutputFilenames.push_back(
        folly::sformat("{}/expected_result_{}.csv", filePrefix, i));
  }

  testCorrectnessDotProductAppHelper(
      serverIp,
      port,
      inputFilenamesAlice,
      outputFilenamesAlice,
      inputFilenamesBob,
      outputFilenamesBob,
      expectedOutputFilenames,
      useTls,
      tlsDir);
}

} // namespace pcf2_dotproduct
