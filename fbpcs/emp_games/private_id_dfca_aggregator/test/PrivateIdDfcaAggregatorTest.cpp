/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <gtest/gtest.h>

#include <cstdint>
#include <filesystem>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include <folly/Format.h>
#include <folly/Random.h>
#include <folly/json.h>

#include <fbpcf/engine/communication/SocketPartyCommunicationAgentFactory.h>
#include <fbpcf/engine/communication/test/AgentFactoryCreationHelper.h>
#include <fbpcf/engine/communication/test/SocketInTestHelper.h>
#include <fbpcf/engine/communication/test/TlsCommunicationUtils.h>
#include <fbpcf/io/api/FileIOWrappers.h>

#include "fbpcs/emp_games/common/Constants.h"
#include "fbpcs/emp_games/private_id_dfca_aggregator/PrivateIdDfcaAggregatorApp.h"

namespace private_id_dfca_aggregator {
class PrivateIdDfcaAggregatorAppTestFixture
    : public ::testing::TestWithParam<std::tuple<bool, int>> {
 protected:
  void SetUp() override {
    std::string filePath = __FILE__;
    epxectedResultsDir_ = filePath.substr(0, filePath.rfind("/")) + "/outputs/";
    tempDir_ = std::filesystem::temp_directory_path();

    tlsDir_ = fbpcf::engine::communication::setUpTlsFiles();
  }

  void TearDown() override {
    fbpcf::engine::communication::deleteTlsFiles(tlsDir_);
  }

  static void runApp(
      std::unique_ptr<
          fbpcf::engine::communication::IPartyCommunicationAgentFactory>
          communicationAgentFactory,
      const std::int32_t party,
      const std::string& inputPath,
      const std::string& outputPath) {
    auto app = std::make_unique<PrivateIdDfcaAggregatorApp>(
        std::move(communicationAgentFactory));

    app->run(party, inputPath, outputPath);
  }

  void runGame(bool useTls, int shardNumber) {
    fbpcf::engine::communication::SocketPartyCommunicationAgent::TlsInfo
        tlsInfo;
    tlsInfo.certPath = useTls ? (tlsDir_ + "/cert.pem") : "";
    tlsInfo.keyPath = useTls ? (tlsDir_ + "/key.pem") : "";
    tlsInfo.passphrasePath = useTls ? (tlsDir_ + "/passphrase.pem") : "";
    tlsInfo.rootCaCertPath = useTls ? (tlsDir_ + "/ca_cert.pem") : "";
    tlsInfo.useTls = useTls;

    auto [communicationAgentFactoryAlice, communicationAgentFactoryBob] =
        fbpcf::engine::communication::getSocketAgentFactoryPair(tlsInfo);

    XLOG(INFO) << "Executing f1";

    std::string outputFile = folly::sformat(
        "{}/result.csv_{}", tempDir_, folly::Random::secureRand64());

    auto f1 = std::async(
        runApp,
        std::move(communicationAgentFactoryAlice),
        common::PUBLISHER,
        "./fbpcs/emp_games/private_id_dfca_aggregator/test/inputs/publisher/shard_" +
            std::to_string(shardNumber) + ".csv",
        outputFile);

    XLOG(INFO) << "Executing f2";

    auto f2 = std::async(
        runApp,
        std::move(communicationAgentFactoryBob),
        common::PARTNER,
        "./fbpcs/emp_games/private_id_dfca_aggregator/test/inputs/partner/shard_" +
            std::to_string(shardNumber) + ".csv",
        outputFile);

    f1.wait();
    f2.wait();

    auto result = fbpcf::io::FileIOWrappers::readFile(outputFile);
    auto expectedResult = fbpcf::io::FileIOWrappers::readFile(
        epxectedResultsDir_ + "expected_result_" + std::to_string(shardNumber) +
        ".csv");

    EXPECT_EQ(result, expectedResult);

    std::filesystem::remove(outputFile);
  }

  std::uint16_t initialPort_;
  std::string epxectedResultsDir_;
  std::string tlsDir_;
  std::string tempDir_;
};

TEST_P(PrivateIdDfcaAggregatorAppTestFixture, testAggregation) {
  auto [useTls, shardNumber] = GetParam();
  runGame(useTls, shardNumber);
}

INSTANTIATE_TEST_CASE_P(
    ShardCombinerAppTest,
    PrivateIdDfcaAggregatorAppTestFixture,
    ::testing::Combine(::testing::Bool(), ::testing::Values(0, 1, 2)),
    [](const testing::TestParamInfo<
        PrivateIdDfcaAggregatorAppTestFixture::ParamType>& info) {
      std::string tls = std::get<0>(info.param) ? "UseTls" : "NoTls";
      auto shardNumber = std::get<1>(info.param);
      std::string name = tls + "_shard_" + std::to_string(shardNumber);
      return name;
    });

} // namespace private_id_dfca_aggregator
