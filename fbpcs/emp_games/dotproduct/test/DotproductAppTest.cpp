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
#include "fbpcf/engine/communication/test/SocketInTestHelper.h"
#include "fbpcf/engine/communication/test/TlsCommunicationUtils.h"

#include "fbpcs/emp_games/common/TestUtil.h"
#include "fbpcs/emp_games/dotproduct/DotproductApp.h"

namespace pcf2_dotproduct {

template <int PARTY, int schedulerId>
static void runGame(
    std::string serverIp,
    int port,
    std::string inputFilePath,
    std::string outputFilePath,
    bool useTls,
    std::string tlsDir) {
  const bool debugMode = false;
  int numFeatures = 50;
  int labelWidth = 16;

  std::map<
      int,
      fbpcf::engine::communication::SocketPartyCommunicationAgentFactory::
          PartyInfo>
      partyInfos({{0, {serverIp, port}}, {1, {serverIp, port}}});

  auto communicationAgentFactory = std::make_unique<
      fbpcf::engine::communication::SocketPartyCommunicationAgentFactory>(
      PARTY, partyInfos, useTls, tlsDir, "dotproduct_traffic_test");

  auto app = std::make_unique<pcf2_dotproduct::DotproductApp<PARTY, PARTY>>(
      std::move(communicationAgentFactory),
      inputFilePath,
      outputFilePath,
      numFeatures,
      labelWidth,
      debugMode);

  app->run();
}

// verify the dotproduct output
bool verifyOutput(
    std::vector<double> result,
    std::vector<double> expectedResult) {
  return std::equal(
      result.begin(),
      result.end(),
      expectedResult.begin(),
      [](double value1, double value2) {
        constexpr double epsilon = 0.00001;
        return std::fabs(value1 - value2) < epsilon;
      });
}

std::vector<double> parseResult(std::string filePath) {
  std::ifstream result;
  std::string line;

  result.open(filePath);
  std::getline(result, line);

  const auto left = line.find('[');
  const auto right = line.find(']');

  std::string valsString = line.substr(left + 1, right - (left + 1));

  std::vector<double> v;

  std::stringstream ss(valsString);

  while (ss.good()) {
    string substr;
    getline(ss, substr, ',');
    v.push_back(std::stod(substr));
  }
  return v;
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
  for (int i = 0; i < inputPathsAlice.size(); i++) {
    auto futureAlice = std::async(
        runGame<0, 0>,
        "",
        port,
        inputPathsAlice.at(i),
        outputPathsAlice.at(i),
        useTls,
        tlsDir);
    auto futureBob = std::async(
        runGame<1, 1>,
        serverIp,
        port,
        inputPathsBob.at(i),
        outputPathsBob.at(i),
        useTls,
        tlsDir);
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
  bool useTls = false;
  std::string tlsDir = "";
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

  int numTestFiles = 2;
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
