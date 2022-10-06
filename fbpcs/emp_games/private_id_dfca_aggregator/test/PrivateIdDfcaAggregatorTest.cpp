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
    : public ::testing::TestWithParam<std::tuple<bool>> {
 protected:
  void SetUp() override {
    std::string filePath = __FILE__;
    baseDir_ = filePath.substr(0, filePath.rfind("/"));
    tempDir_ = std::filesystem::temp_directory_path();

    tlsDir_ = fbpcf::engine::communication::setUpTlsFiles();
  }

  void TearDown() override {
    fbpcf::engine::communication::deleteTlsFiles(tlsDir_);
  }

  static void doIt(
      std::unique_ptr<
          fbpcf::engine::communication::IPartyCommunicationAgentFactory>
          communicationAgentFactory,
      const std::int32_t party,
      const std::int32_t numShards,
      const std::int32_t shardStartIndex,
      const std::string& inputPath,
      const std::string& inputFilePrefix,
      const std::string& outputPath) {
    auto app = std::make_unique<PrivateIdDfcaAggregatorApp>(
        std::move(communicationAgentFactory));

    app->run(
        party,
        numShards,
        shardStartIndex,
        inputPath,
        inputFilePrefix,
        outputPath);
  }

  void runGame(bool useTls) {
    std::string outputPath =
        "./fbpcs/emp_games/private_id_dfca_aggregator/test/";

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

    auto f1 = std::async(
        doIt,
        std::move(communicationAgentFactoryAlice),
        common::PUBLISHER,
        3,
        0,
        "./fbpcs/emp_games/private_id_dfca_aggregator/test/shards/publisher",
        "shard",
        outputPath + "result.csv");

    XLOG(INFO) << "Executing f2";

    auto f2 = std::async(
        doIt,
        std::move(communicationAgentFactoryBob),
        common::PARTNER,
        2,
        3,
        "./fbpcs/emp_games/private_id_dfca_aggregator/test/shards/partner",
        "shard",
        outputPath + "result.csv");

    f1.wait();
    f2.wait();

    auto result =
        fbpcf::io::FileIOWrappers::readFile(outputPath + "result.csv");
    auto expectedResult =
        fbpcf::io::FileIOWrappers::readFile(outputPath + "expected_result.csv");

    EXPECT_EQ(result, expectedResult);

    std::filesystem::remove(outputPath + "result.csv");
  }

  std::uint16_t initialPort_;
  std::string baseDir_;
  std::string tlsDir_;
  std::string tempDir_;
};

TEST_P(PrivateIdDfcaAggregatorAppTestFixture, testAggregation) {
  auto [useTls] = GetParam();
  runGame(useTls);
}

INSTANTIATE_TEST_CASE_P(
    ShardCombinerAppTest,
    PrivateIdDfcaAggregatorAppTestFixture,
    ::testing::Combine(::testing::Bool()),
    [](const testing::TestParamInfo<
        PrivateIdDfcaAggregatorAppTestFixture::ParamType>& info) {
      std::string tls = std::get<0>(info.param) ? "UseTls" : "NoTls";
      std::string name = tls;
      return name;
    });

} // namespace private_id_dfca_aggregator
