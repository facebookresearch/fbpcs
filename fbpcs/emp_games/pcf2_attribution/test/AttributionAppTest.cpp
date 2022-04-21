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

#include "fbpcf/engine/communication/SocketPartyCommunicationAgentFactory.h"
#include "fbpcf/engine/communication/test/TlsCommunicationUtils.h"
#include "fbpcs/emp_games/common/Constants.h"
#include "fbpcs/emp_games/common/TestUtil.h"
#include "fbpcs/emp_games/pcf2_attribution/test/AttributionTestUtils.h"

namespace pcf2_attribution {

template <
    int PARTY,
    int schedulerId,
    bool usingBatch,
    common::InputEncryption inputEncryption>
static void runGame(
    const std::string& serverIp,
    const uint16_t port,
    const std::string& attributionRules,
    const std::filesystem::path& inputPath,
    const std::string& outputPath,
    bool useTls,
    const std::string& tlsDir) {
  std::map<
      int,
      fbpcf::engine::communication::SocketPartyCommunicationAgentFactory::
          PartyInfo>
      partyInfos({{0, {serverIp, port}}, {1, {serverIp, port}}});

  auto communicationAgentFactory = std::make_unique<
      fbpcf::engine::communication::SocketPartyCommunicationAgentFactory>(
      PARTY, partyInfos, useTls, tlsDir);

  AttributionApp<PARTY, schedulerId, usingBatch, inputEncryption>(
      std::move(communicationAgentFactory),
      attributionRules,
      std::vector<string>{inputPath},
      std::vector<string>{outputPath})
      .run();
}

// helper function for executing MPC game and verifying corresponding output
template <int id, bool usingBatch, common::InputEncryption inputEncryption>
inline void testCorrectnessAttributionAppHelper(
    std::string serverIpAlice,
    int16_t portAlice,
    std::vector<std::string> attributionRule,
    std::vector<std::string> inputPathAlice,
    std::vector<std::string> outputPathAlice,
    std::string serverIpBob,
    int16_t portBob,
    std::vector<std::string> inputPathBob,
    std::vector<std::string> outputPathBob,
    std::vector<std::string> expectedOutputFilenames,
    bool useTls,
    std::string& tlsDir) {
  auto futureAlice = std::async(
      runGame<common::PUBLISHER, 2 * id, usingBatch, inputEncryption>,
      serverIpAlice,
      portAlice + 100 * id,
      attributionRule.at(id),
      inputPathAlice.at(id),
      outputPathAlice.at(id),
      useTls,
      tlsDir);
  auto futureBob = std::async(
      runGame<common::PARTNER, 2 * id + 1, usingBatch, inputEncryption>,
      serverIpBob,
      portBob + 100 * id,
      "",
      inputPathBob.at(id),
      outputPathBob.at(id),
      useTls,
      tlsDir);

  futureAlice.wait();
  futureBob.wait();

  auto resAlice = AttributionOutputMetrics::fromJson(
      fbpcf::io::read(outputPathAlice.at(id)));
  auto resBob =
      AttributionOutputMetrics::fromJson(fbpcf::io::read(outputPathBob.at(id)));

  auto result = revealXORedResult(resAlice, resBob, attributionRule.at(id));

  verifyOutput(result, expectedOutputFilenames.at(id));
}

class AttributionAppTest
    : public ::testing::TestWithParam<
          std::tuple<int, bool, bool>> { // id, usingBatch, useTls
 protected:
  void SetUp() override {
    tlsDir_ = fbpcf::engine::communication::setUpTlsFiles();
    port_ = 5000 + folly::Random::rand32() % 1000;
    std::string baseDir_ =
        private_measurement::test_util::getBaseDirFromPath(__FILE__);
    std::string tempDir = std::filesystem::temp_directory_path();
    serverIpAlice_ = "";
    serverIpBob_ = "127.0.0.1";
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

  template <int id, bool usingBatch>
  void testCorrectnessAttributionAppWrapper(bool useTls) {
    testCorrectnessAttributionAppHelper<
        id,
        usingBatch,
        common::InputEncryption::Plaintext>(
        serverIpAlice_,
        port_,
        attributionRules_,
        inputFilenamesAlice_,
        outputFilenamesAlice_,
        serverIpBob_,
        port_,
        inputFilenamesBob_,
        outputFilenamesBob_,
        expectedOutputFilenames_,
        useTls,
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
  auto [id, usingBatch, useTls] = GetParam();

  switch (id) {
    case 0:
      if (usingBatch) {
        testCorrectnessAttributionAppWrapper<0, true>(useTls);
      } else {
        testCorrectnessAttributionAppWrapper<0, false>(useTls);
      }
      break;
    case 1:
      if (usingBatch) {
        testCorrectnessAttributionAppWrapper<1, true>(useTls);
      } else {
        testCorrectnessAttributionAppWrapper<1, false>(useTls);
      }
      break;
    case 2:
      if (usingBatch) {
        testCorrectnessAttributionAppWrapper<2, true>(useTls);
      } else {
        testCorrectnessAttributionAppWrapper<2, false>(useTls);
      }
      break;
    case 3:
      if (usingBatch) {
        testCorrectnessAttributionAppWrapper<3, true>(useTls);
      } else {
        testCorrectnessAttributionAppWrapper<3, false>(useTls);
      }
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
      auto batch = std::get<1>(info.param) ? "True" : "False";
      auto tls = std::get<2>(info.param) ? "True" : "False";

      std::string name = "ID_" + id + "_Batch_" + batch + "_TLS_" + tls;
      return name;
    });

} // namespace pcf2_attribution
