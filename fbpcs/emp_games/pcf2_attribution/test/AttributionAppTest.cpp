/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <math.h>
#include <filesystem>
#include <string>

#include <gtest/gtest.h>
#include "folly/Format.h"
#include "folly/Random.h"
#include "folly/logging/xlog.h"

#include <fbpcf/io/api/FileIOWrappers.h>
#include "fbpcf/engine/communication/SocketPartyCommunicationAgentFactory.h"
#include "fbpcf/engine/communication/test/AgentFactoryCreationHelper.h"
#include "fbpcf/engine/communication/test/SocketInTestHelper.h"
#include "fbpcf/engine/communication/test/TlsCommunicationUtils.h"
#include "fbpcs/emp_games/common/Constants.h"
#include "fbpcs/emp_games/common/TestUtil.h"
#include "fbpcs/emp_games/pcf2_attribution/test/AttributionTestUtils.h"

namespace pcf2_attribution {

template <int PARTY, int schedulerId>
static void runGame(
    bool useXorEncryption,
    common::InputEncryption inputEncryption,
    const std::string& attributionRules,
    const std::filesystem::path& inputPath,
    const std::string& outputPath,
    std::unique_ptr<
        fbpcf::engine::communication::IPartyCommunicationAgentFactory>
        communicationAgentFactory) {
  auto metricCollector =
      std::make_shared<fbpcf::util::MetricCollector>("attribution_test");
  AttributionApp<PARTY, schedulerId>(
      std::move(communicationAgentFactory),
      attributionRules,
      std::vector<string>{inputPath},
      std::vector<string>{outputPath},
      metricCollector,
      useXorEncryption,
      inputEncryption)
      .run();
}

// helper function for executing MPC game and verifying corresponding output
template <int id>
inline void testCorrectnessAttributionAppHelper(
    std::vector<std::string> attributionRule,
    std::vector<std::string> inputPathAlice,
    std::vector<std::string> outputPathAlice,
    std::vector<std::string> inputPathBob,
    std::vector<std::string> outputPathBob,
    std::vector<std::string> expectedOutputFilenames,
    bool useTls,
    bool useXorEncryption,
    common::InputEncryption inputEncryption,
    std::string& tlsDir) {
  fbpcf::engine::communication::SocketPartyCommunicationAgent::TlsInfo tlsInfo;
  tlsInfo.certPath = useTls ? (tlsDir + "/cert.pem") : "";
  tlsInfo.keyPath = useTls ? (tlsDir + "/key.pem") : "";
  tlsInfo.passphrasePath = useTls ? (tlsDir + "/passphrase.pem") : "";
  tlsInfo.rootCaCertPath = useTls ? (tlsDir + "/ca_cert.pem") : "";
  tlsInfo.useTls = useTls;

  auto [communicationAgentFactoryAlice, communicationAgentFactoryBob] =
      fbpcf::engine::communication::getSocketAgentFactoryPair(tlsInfo);

  auto futureAlice = std::async(
      runGame<common::PUBLISHER, 2 * id>,
      useXorEncryption,
      inputEncryption,
      attributionRule.at(id),
      inputPathAlice.at(id),
      outputPathAlice.at(id),
      std::move(communicationAgentFactoryAlice));
  auto futureBob = std::async(
      runGame<common::PARTNER, 2 * id + 1>,
      useXorEncryption,
      inputEncryption,
      "",
      inputPathBob.at(id),
      outputPathBob.at(id),
      std::move(communicationAgentFactoryBob));

  futureAlice.wait();
  futureBob.wait();

  auto resAlice = AttributionOutputMetrics::fromJson(
      fbpcf::io::FileIOWrappers::readFile(outputPathAlice.at(id)));
  auto resBob = AttributionOutputMetrics::fromJson(
      fbpcf::io::FileIOWrappers::readFile(outputPathBob.at(id)));

  auto result = revealXORedResult(resAlice, resBob, attributionRule.at(id));

  verifyOutput(result, expectedOutputFilenames.at(id));
}

class AttributionAppTest
    : public ::testing::TestWithParam<
          std::tuple<int, bool, bool>> { // id, useTls,
                                         // use_encrypt_xor_not,
 protected:
  void SetUp() override {
    tlsDir_ = fbpcf::engine::communication::setUpTlsFiles();
    std::string baseDir_ =
        private_measurement::test_util::getBaseDirFromPath(__FILE__);
    std::string tempDir = std::filesystem::temp_directory_path();
    std::string outputPathAlice_ = folly::sformat(
        "{}/output_path_alice.json_{}", tempDir, folly::Random::secureRand64());
    std::string outputPathBob_ = folly::sformat(
        "{}/output_path_bob.json_{}", tempDir, folly::Random::secureRand64());

    attributionRules_ = std::vector<string>{
        common::LAST_CLICK_1D,
        common::LAST_TOUCH_1D,
        common::LAST_CLICK_2_7D,
        common::LAST_TOUCH_2_7D};

    for (size_t i = 0; i < attributionRules_.size(); ++i) {
      auto attributionRule = attributionRules_.at(i);
      std::string filePrefix = baseDir_ + "test_correctness/" + attributionRule;
      inputFilenamesAlice_.push_back(filePrefix + ".publisher.csv");
      inputFilenamesBob_.push_back(filePrefix + ".partner.csv");
      outputFilenamesAlice_.push_back(outputPathAlice_ + attributionRule);
      outputFilenamesBob_.push_back(outputPathBob_ + attributionRule);
      expectedOutputFilenames_.push_back(filePrefix + ".json");
    }
  }

  void TearDown() override {
    std::filesystem::remove(outputPathAlice_);
    std::filesystem::remove(outputPathBob_);
    fbpcf::engine::communication::deleteTlsFiles(tlsDir_);
  }

  template <int id>
  void testCorrectnessAttributionAppWrapper(
      bool useTls,
      bool useXorEncryption) {
    testCorrectnessAttributionAppHelper<id>(
        attributionRules_,
        inputFilenamesAlice_,
        outputFilenamesAlice_,
        inputFilenamesBob_,
        outputFilenamesBob_,
        expectedOutputFilenames_,
        useTls,
        useXorEncryption,
        common::InputEncryption::Plaintext,
        tlsDir_);
  }

  std::string serverIpAlice_;
  std::string serverIpBob_;
  uint16_t port_;
  std::string outputPathAlice_;
  std::string outputPathBob_;
  std::vector<std::string> attributionRules_;
  std::vector<std::string> inputFilenamesAlice_;
  std::vector<std::string> inputFilenamesBob_;
  std::vector<std::string> outputFilenamesAlice_;
  std::vector<std::string> outputFilenamesBob_;
  std::vector<std::string> expectedOutputFilenames_;
  std::string tlsDir_;
};

TEST_P(AttributionAppTest, TestCorrectness) {
  auto [id, useTls, useXorEncryption] = GetParam();

  switch (id) {
    case 0:

      testCorrectnessAttributionAppWrapper<0>(useTls, useXorEncryption);

      break;
    case 1:

      testCorrectnessAttributionAppWrapper<1>(useTls, useXorEncryption);

      break;
    case 2:

      testCorrectnessAttributionAppWrapper<2>(useTls, useXorEncryption);

      break;
    case 3:

      testCorrectnessAttributionAppWrapper<3>(useTls, useXorEncryption);

      break;
    default:
      break;
  }
}

// Test cases are iterate in https://fb.quip.com/IUHDApxKEAli
INSTANTIATE_TEST_SUITE_P(
    AttributionAppTest,
    AttributionAppTest,
    // the first parameter is an ID that is used to index into an
    // array for attribution rules and input/output file names.
    // See the class above for how the ID is used.
    ::testing::Combine(
        ::testing::Values(0, 1, 2, 3),
        ::testing::Bool(),
        ::testing::Bool()),

    [](const testing::TestParamInfo<AttributionAppTest::ParamType>& info) {
      auto id = std::to_string(std::get<0>(info.param));

      auto useXorEncryption = std::get<1>(info.param) ? "True" : "False";
      auto tls = std::get<2>(info.param) ? "True" : "False";
      std::string name =
          "ID_" + id + "_TLS_" + tls + "_useXorEncryption_" + useXorEncryption;
      return name;
    });

} // namespace pcf2_attribution
