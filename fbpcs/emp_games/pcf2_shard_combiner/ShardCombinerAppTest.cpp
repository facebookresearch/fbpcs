/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <cstdint>
#include <filesystem>
#include <memory>
#include <thread>
#include <vector>

#include <gtest/gtest.h>

#include <folly/Format.h>
#include <folly/Random.h>
#include <folly/json.h>

#include <fbpcf/engine/communication/SocketPartyCommunicationAgentFactory.h>
#include <fbpcf/engine/communication/test/SocketInTestHelper.h>
#include <fbpcf/engine/communication/test/TlsCommunicationUtils.h>
#include <fbpcf/io/api/FileIOWrappers.h>
#include "fbpcf/engine/communication/test/AgentFactoryCreationHelper.h"

#include "fbpcs/emp_games/common/Constants.h"
#include "fbpcs/emp_games/pcf2_shard_combiner/AggMetrics.h"
#include "fbpcs/emp_games/pcf2_shard_combiner/ShardCombinerApp.h"
#include "fbpcs/emp_games/pcf2_shard_combiner/ShardValidator.h"

namespace shard_combiner {

template <
    ShardSchemaType shardSchemaType,
    int32_t schedulerId,
    bool usingBatch,
    common::InputEncryption inputEncryption>
static void runOneParty(
    int32_t firstShardIndex,
    int32_t numShards,
    int64_t threshold,
    const std::string& inputPath,
    const std::string& inputPrefix,
    const std::string& outputPath,
    bool xorEncrypted,
    common::ResultVisibility resultVisibility,
    std::unique_ptr<
        fbpcf::engine::communication::IPartyCommunicationAgentFactory>
        communicationAgentFactory) {
  auto metricCollector =
      std::make_shared<fbpcf::util::MetricCollector>("shard_combiner_test");
  ShardCombinerApp<shardSchemaType, schedulerId, usingBatch, inputEncryption>(
      std::move(communicationAgentFactory),
      numShards,
      firstShardIndex,
      inputPath,
      inputPrefix,
      outputPath,
      threshold,
      xorEncrypted,
      resultVisibility,
      metricCollector)
      .run();
}

template <
    ShardSchemaType shardSchemaType,
    bool usingBatch,
    common::InputEncryption inputEncryption>
static void runGame(
    int32_t firstShardIndex,
    int32_t numShards,
    int64_t threshold,
    const std::string& baseDir,
    const std::string& inputFilePrefixPartner,
    const std::string& inputFilePrefixPublisher,
    const std::string& expectedOutputFile,
    bool useTls,
    const std::string& tlsDir,
    bool xorEncrypted,
    common::ResultVisibility resultVisibility,
    std::string& tempDir) {
  std::string outputPathPartner = folly::sformat(
      "{}/output_path_partner.json_{}", tempDir, folly::Random::secureRand64());
  std::string outputPathPublisher = folly::sformat(
      "{}/output_path_publisher.json_{}",
      tempDir,
      folly::Random::secureRand64());

  fbpcf::engine::communication::SocketPartyCommunicationAgent::TlsInfo tlsInfo;
  tlsInfo.certPath = useTls ? (tlsDir + "/cert.pem") : "";
  tlsInfo.keyPath = useTls ? (tlsDir + "/key.pem") : "";
  tlsInfo.passphrasePath = useTls ? (tlsDir + "/passphrase.pem") : "";
  tlsInfo.rootCaCertPath = useTls ? (tlsDir + "/ca_cert.pem") : "";
  tlsInfo.useTls = useTls;

  auto [communicationAgentFactoryAlice, communicationAgentFactoryBob] =
      fbpcf::engine::communication::getSocketAgentFactoryPair(tlsInfo);

  auto f1 = std::async(
      runOneParty<
          shardSchemaType,
          common::PUBLISHER,
          usingBatch,
          inputEncryption>,
      firstShardIndex,
      numShards,
      threshold,
      baseDir,
      inputFilePrefixPublisher,
      outputPathPublisher,
      xorEncrypted,
      resultVisibility,
      std::move(communicationAgentFactoryAlice));

  auto f2 = std::async(
      runOneParty<
          shardSchemaType,
          common::PARTNER,
          usingBatch,
          inputEncryption>,
      firstShardIndex,
      numShards,
      threshold,
      baseDir,
      inputFilePrefixPartner,
      outputPathPartner,
      xorEncrypted,
      resultVisibility,
      std::move(communicationAgentFactoryBob));

  f1.wait();
  f2.wait();
  auto expectedObj = folly::parseJson(fbpcf::io::FileIOWrappers::readFile(
      folly::sformat("{}", expectedOutputFile)));

  // Check if the results match only in the party has visibility.
  if (resultVisibility == common::ResultVisibility::kPublic ||
      resultVisibility == common::ResultVisibility::kPublisher) {
    EXPECT_EQ(
        expectedObj,
        folly::parseJson(fbpcf::io::FileIOWrappers::readFile(
            folly::sformat("{}", outputPathPublisher))));
  } else {
    EXPECT_NE(
        expectedObj,
        folly::parseJson(fbpcf::io::FileIOWrappers::readFile(
            folly::sformat("{}", outputPathPublisher))));
  }

  // Check if the results match only in the party has visibility.
  if (resultVisibility == common::ResultVisibility::kPublic ||
      resultVisibility == common::ResultVisibility::kPartner) {
    EXPECT_EQ(
        expectedObj,
        folly::parseJson(fbpcf::io::FileIOWrappers::readFile(
            folly::sformat("{}", outputPathPartner))));
  } else {
    EXPECT_NE(
        expectedObj,
        folly::parseJson(fbpcf::io::FileIOWrappers::readFile(
            folly::sformat("{}", outputPathPartner))));
  }
  std::filesystem::remove(outputPathPartner);
  std::filesystem::remove(outputPathPublisher);
}
class ShardCombinerAppTestFixture
    : public ::testing::TestWithParam<std::tuple<
          bool, /* useTls */
          bool, /* usingBatch */
          bool, /* xorEncryption */
          std::tuple<
              std::string,
              std::string,
              std::string>, /* (publisherInputFileName,
                              partnerInputFileName,
                              expectedOutputFileName)
                            */
          common::ResultVisibility>> {
 protected:
  void SetUp() override {
    std::string filePath = __FILE__;
    baseDir_ = filePath.substr(0, filePath.rfind("/")) + "/test/";
    tempDir_ = std::filesystem::temp_directory_path();

    tlsDir_ = fbpcf::engine::communication::setUpTlsFiles();
  }

  void TearDown() override {
    fbpcf::engine::communication::deleteTlsFiles(tlsDir_);
  }

  std::uint16_t initialPort_;
  std::string baseDir_;
  std::string tlsDir_;
  std::string tempDir_;
};

TEST_P(ShardCombinerAppTestFixture, TestCorrectness) {
  auto [useTls, usingBatch, xorEncrypted, fileNames, resultVisibility] =
      GetParam();
  auto threshold = 0;

  if (std::get<1>(fileNames).find("attribution") != std::string::npos) {
    if (std::get<1>(fileNames).find("kanon") != std::string::npos) {
      threshold = 100;
    }

    if (usingBatch) {
      runGame<
          ShardSchemaType::kAdObjFormat,
          true, // usingBatch
          common::InputEncryption::Xor>(
          0, // firstShardIndex
          2, // numShards
          threshold,
          baseDir_ + "ad_object_format",
          std::get<0>(fileNames),
          std::get<1>(fileNames),
          baseDir_ + "expected_shard_aggregator_correctness_test/" +
              std::get<2>(fileNames),
          useTls,
          tlsDir_,
          xorEncrypted,
          resultVisibility,
          tempDir_);
    } else {
      runGame<
          ShardSchemaType::kAdObjFormat,
          false, // usingBatch
          common::InputEncryption::Xor>(
          0, // firstShardIndex
          2, // numShards
          threshold,
          baseDir_ + "ad_object_format",
          std::get<0>(fileNames),
          std::get<1>(fileNames),
          baseDir_ + "expected_shard_aggregator_correctness_test/" +
              std::get<2>(fileNames),
          useTls,
          tlsDir_,
          xorEncrypted,
          resultVisibility,
          tempDir_);
    }
  } else {
    threshold = 100;
    if (usingBatch) {
      runGame<
          ShardSchemaType::kGroupedLiftMetrics,
          true, // usingBatch
          common::InputEncryption::Xor>(
          0, // firstShardIndex
          2, // numShards
          threshold,
          baseDir_ + "lift_threshold_test",
          std::get<0>(fileNames),
          std::get<1>(fileNames),
          baseDir_ + "lift_threshold_test/" + std::get<2>(fileNames),
          useTls,
          tlsDir_,
          xorEncrypted,
          resultVisibility,
          tempDir_);
    } else {
      runGame<
          ShardSchemaType::kGroupedLiftMetrics,
          false, // usingBatch
          common::InputEncryption::Xor>(
          0, // firstShardIndex
          2, // numShards
          threshold,
          baseDir_ + "lift_threshold_test",
          std::get<0>(fileNames),
          std::get<1>(fileNames),
          baseDir_ + "lift_threshold_test/" + std::get<2>(fileNames),
          useTls,
          tlsDir_,
          xorEncrypted,
          resultVisibility,
          tempDir_);
    }
  }
}

INSTANTIATE_TEST_CASE_P(
    ShardCombinerAppTest,
    ShardCombinerAppTestFixture,
    ::testing::Combine(
        ::testing::Bool(),
        ::testing::Bool(),
        ::testing::Bool(),
        ::testing::Values(
            std::tuple(
                "publisher_attribution_correctness_old_out.json",
                "partner_attribution_correctness_old_out.json",
                "expected_shard_aggregator_correctness_old_out.json"),
            std::tuple(
                "publisher_attribution_correctness_mmt_nooverlap_out.json",
                "partner_attribution_correctness_mmt_nooverlap_out.json",
                "expected_shard_aggregator_correctness_mmt_nooverlap_out.json"),
            std::tuple(
                "publisher_attribution_correctness_mmt_overlap_out.json",
                "partner_attribution_correctness_mmt_overlap_out.json",
                "expected_shard_aggregator_correctness_mmt_overlap_out.json"),
            std::tuple(
                "publisher_attribution_correctness_clickonly_touchonly_out.json",
                "partner_attribution_correctness_clickonly_touchonly_out.json",
                "expected_shard_aggregator_correctness_clickonly_touchonly_out.json"),
            std::tuple(
                "publisher_attribution_correctness_clicktouch_touchonly_out.json",
                "partner_attribution_correctness_clicktouch_touchonly_out.json",
                "expected_shard_aggregator_correctness_clicktouch_touchonly_out.json"),
            std::tuple(
                "publisher_attribution_correctness_clickonly_clicktouch_out.json",
                "partner_attribution_correctness_clickonly_clicktouch_out.json",
                "expected_shard_aggregator_correctness_clickonly_clicktouch_out.json"),
            std::tuple(
                "publisher_attribution_correctness_clicktouch_clicktouch_out.json",
                "partner_attribution_correctness_clicktouch_clicktouch_out.json",
                "expected_shard_aggregator_correctness_clicktouch_clicktouch_out.json"),
            std::tuple(
                "publisher_attribution_correctness_kanonymity_allpass_out.json",
                "partner_attribution_correctness_kanonymity_allpass_out.json",
                "expected_shard_aggregator_correctness_kanonymity_allpass_out.json"),
            std::tuple(
                "publisher_lift_input_shard.json",
                "partner_lift_input_shard.json",
                "lift_expected_output_shards_2.json")),
        ::testing::Values(
            common::ResultVisibility::kPartner,
            common::ResultVisibility::kPublisher,
            common::ResultVisibility::kPublic)),
    [](const testing::TestParamInfo<ShardCombinerAppTestFixture::ParamType>&
           info) {
      std::string tls = std::get<0>(info.param) ? "UseTls" : "NoTls";
      std::string usingBatch =
          std::get<1>(info.param) ? "UsingBatch" : "NoBatch";
      std::string xorEncrypted =
          std::get<2>(info.param) ? "XorEncrypted" : "NoXorEncrypted";
      auto resultVisibilityEnum = std::get<4>(info.param);
      std::string resultVisibility = "";
      switch (resultVisibilityEnum) {
        case common::ResultVisibility::kPublic:
          resultVisibility = "Public";
          break;
        case common::ResultVisibility::kPartner:
          resultVisibility = "Partner";
          break;
        case common::ResultVisibility::kPublisher:
          resultVisibility = "Publisher";
          break;
      }

      std::string gameType = "";
      std::string testCase = "";
      if (std::get<1>(std::get<3>(info.param)).find("attribution") !=
          std::string::npos) {
        gameType = "Attribution";
        auto endIndex = std::get<1>(std::get<3>(info.param)).find("_out.json");
        auto startIndex = 32;
        testCase = std::get<1>(std::get<3>(info.param))
                       .substr(startIndex, endIndex - startIndex);
      } else {
        gameType = "Lift";
        testCase = "default";
      }

      std::string name = tls + "_" + usingBatch + "_" + xorEncrypted + "_" +
          gameType + "_" + testCase + "_" + resultVisibility;
      return name;
    });

} // namespace shard_combiner
