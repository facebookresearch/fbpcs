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

#include "fbpcs/emp_games/common/Constants.h"
#include "fbpcs/emp_games/pcf2_shard_combiner/AggMetrics.h"
#include "fbpcs/emp_games/pcf2_shard_combiner/ShardCombinerApp.h"
#include "fbpcs/emp_games/pcf2_shard_combiner/ShardValidator.h"

namespace shard_combiner {
class ShardCombinerAppTestFixture
    : public ::testing::TestWithParam<std::tuple<bool, bool>> {
 protected:
  void SetUp() override {
    // When running tests in parallel there could be contention for ports
    // to run the game. To be on the safer side we set different ports
    // for each test.
    initialPort_ =
        fbpcf::engine::communication::SocketInTestHelper::findNextOpenPort(
            5500);
    std::string filePath = __FILE__;
    baseDir_ = filePath.substr(0, filePath.rfind("/")) + "/test/";
    tempDir_ = std::filesystem::temp_directory_path();

    tlsDir_ = fbpcf::engine::communication::setUpTlsFiles();
  }

  void TearDown() override {
    fbpcf::engine::communication::deleteTlsFiles(tlsDir_);
  }

  template <
      ShardSchemaType shardSchemaType,
      bool usingBatch,
      common::InputEncryption inputEncryption>
  void runGame(
      int32_t firstShardIndex,
      int32_t numShards,
      int64_t threshold,
      const std::string& baseDir,
      const std::string& inputFilePrefixPartner,
      const std::string& inputFilePrefixPublisher,
      const std::string& expectedOutputFile,
      bool useTls,
      const std::string& tlsDir,
      uint16_t portNo,
      bool xorEncrypted,
      common::ResultVisibility resultVisibility) {
    std::string outputPathPartner = folly::sformat(
        "{}/output_path_partner.json_{}",
        tempDir_,
        folly::Random::secureRand64());
    std::string outputPathPublisher = folly::sformat(
        "{}/output_path_publisher.json_{}",
        tempDir_,
        folly::Random::secureRand64());

    std::pair<std::string, std::uint16_t> publisherEndpoint = {
        "127.0.0.1", portNo};
    std::pair<std::string, std::uint16_t> partnerEndpoint = {
        "127.0.0.1", (portNo)};

    auto f1 = std::async(
        doIt<shardSchemaType, common::PUBLISHER, usingBatch, inputEncryption>,
        firstShardIndex,
        numShards,
        threshold,
        publisherEndpoint,
        baseDir,
        inputFilePrefixPublisher,
        outputPathPublisher,
        useTls,
        tlsDir,
        xorEncrypted,
        resultVisibility);

    auto f2 = std::async(
        doIt<shardSchemaType, common::PARTNER, usingBatch, inputEncryption>,
        firstShardIndex,
        numShards,
        threshold,
        partnerEndpoint,
        baseDir,
        inputFilePrefixPartner,
        outputPathPartner,
        useTls,
        tlsDir,
        xorEncrypted,
        resultVisibility);

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

  template <
      ShardSchemaType shardSchemaType,
      int32_t schedulerId,
      bool usingBatch,
      common::InputEncryption inputEncryption>
  static void doIt(
      int32_t firstShardIndex,
      int32_t numShards,
      int64_t threshold,
      const std::pair<std::string, std::uint16_t>& endPoint,
      const std::string& inputPath,
      const std::string& inputPrefix,
      const std::string& outputPath,
      bool useTls,
      const std::string& tlsDir,
      bool xorEncrypted,
      common::ResultVisibility resultVisibility) {
    std::map<
        int32_t,
        fbpcf::engine::communication::SocketPartyCommunicationAgentFactory::
            PartyInfo>
        partyInfos(
            {{0, {endPoint.first, endPoint.second}},
             {1, {endPoint.first, endPoint.second}}});

    auto communicationAgentFactory = std::make_unique<
        fbpcf::engine::communication::SocketPartyCommunicationAgentFactory>(
        schedulerId, partyInfos, useTls, tlsDir, "shard_combiner_test_traffic");

    ShardCombinerApp<shardSchemaType, schedulerId, usingBatch, inputEncryption>(
        std::move(communicationAgentFactory),
        numShards,
        firstShardIndex,
        inputPath,
        inputPrefix,
        outputPath,
        threshold,
        xorEncrypted,
        resultVisibility)
        .run();
  }

  void testForShortCase(
      std::string caseShortName,
      bool xorEncrypted,
      bool usingBatch,
      bool useTls,
      std::uint16_t portNo) {
    const std::string inputPathAlice =
        "publisher_attribution_correctness_" + caseShortName + "_out.json";
    const std::string inputPathBob =
        "partner_attribution_correctness_" + caseShortName + "_out.json";
    const std::string expectedOutPath = baseDir_ +
        "expected_shard_aggregator_correctness_test/expected_shard_aggregator_correctness_" +
        caseShortName + "_out.json";

    // Also test if cases work various visibility
    std::vector<common::ResultVisibility> visibilityTypes{
        common::ResultVisibility::kPublic,
        common::ResultVisibility::kPartner,
        common::ResultVisibility::kPublisher};

    // For normal tests, set kanonymity threshold to zero
    auto kanonymityThreshold = 0;
    // For k-anonymity dedicated tests, set threshold to 100
    if (caseShortName.find("kanonymity") != std::string::npos) {
      kanonymityThreshold = 100;
    }
    for (auto visibility : visibilityTypes) {
      if (usingBatch) {
        runGame<
            ShardSchemaType::kAdObjFormat,
            true, // usingBatch
            common::InputEncryption::Xor>(
            0, // firstShardIndex
            2, // numShards
            kanonymityThreshold,
            baseDir_ + "ad_object_format",
            inputPathAlice,
            inputPathBob,
            expectedOutPath,
            useTls,
            tlsDir_,
            portNo,
            xorEncrypted,
            common::ResultVisibility::kPublic);
      } else {
        runGame<
            ShardSchemaType::kAdObjFormat,
            false, // usingBatch
            common::InputEncryption::Xor>(
            0, // firstShardIndex
            2, // numShards
            kanonymityThreshold,
            baseDir_ + "ad_object_format",
            inputPathAlice,
            inputPathBob,
            expectedOutPath,
            useTls,
            tlsDir_,
            portNo,
            xorEncrypted,
            common::ResultVisibility::kPublic);
      }
    }
  }

  void testLift(
      bool xorEncrypted,
      bool usingBatch,
      bool useTls,
      std::uint16_t portNo) {
    const std::string partnerFileName = "partner_lift_input_shard.json";
    const std::string publisherFileName = "publisher_lift_input_shard.json";
    std::string expectedOutFileName = "lift_expected_output_shards_2.json";

    int64_t kanonymityThreshold = 100;

    if (usingBatch) {
      runGame<
          ShardSchemaType::kGroupedLiftMetrics,
          true,
          common::InputEncryption::Xor>(
          0,
          2, // numShards
          kanonymityThreshold,
          baseDir_ + "lift_threshold_test",
          partnerFileName,
          publisherFileName,
          baseDir_ + "/lift_threshold_test/" + expectedOutFileName,
          useTls,
          tlsDir_,
          portNo,
          xorEncrypted,
          common::ResultVisibility::kPublic);
    } else {
      runGame<
          ShardSchemaType::kGroupedLiftMetrics,
          false,
          common::InputEncryption::Xor>(
          0,
          2, // numShards
          kanonymityThreshold,
          baseDir_ + "lift_threshold_test",
          partnerFileName,
          publisherFileName,
          baseDir_ + "/lift_threshold_test/" + expectedOutFileName,
          useTls,
          tlsDir_,
          portNo,
          xorEncrypted,
          common::ResultVisibility::kPublic);
    }
  }

  std::uint16_t initialPort_;
  std::string baseDir_;
  std::string tlsDir_;
  std::string tempDir_;
};

// Test cases are iterate in https://fb.quip.com/IUHDApxKEAli
// -BEGIN- AdObject format related tests --
// --- ONE TH ---
TEST_P(ShardCombinerAppTestFixture, TestGenericShardAggCorrectnessAdObject) {
  auto [useTls, usingBatch] = GetParam();
  testForShortCase(
      "old", false /* XorEncrypted */, usingBatch, useTls, initialPort_);
  testForShortCase(
      "mmt_nooverlap",
      false /* XorEncrypted */,
      usingBatch,
      useTls,
      initialPort_);
  testForShortCase(
      "mmt_overlap",
      false /* XorEncrypted */,
      usingBatch,
      useTls,
      initialPort_);
  testForShortCase(
      "clickonly_touchonly",
      false /* XorEncrypted */,
      usingBatch,
      useTls,
      initialPort_);
  testForShortCase(
      "clicktouch_touchonly",
      false /* XorEncrypted */,
      usingBatch,
      useTls,
      initialPort_);
  testForShortCase(
      "clickonly_clicktouch",
      false /* XorEncrypted */,
      usingBatch,
      useTls,
      initialPort_);
  testForShortCase(
      "clicktouch_clicktouch",
      false /* XorEncrypted */,
      usingBatch,
      useTls,
      initialPort_);
  testForShortCase(
      "kanonymity_allpass",
      false /* XorEncrypted */,
      usingBatch,
      useTls,
      initialPort_);
}

// --- adObjXor ---
TEST_P(
    ShardCombinerAppTestFixture,
    TestGenericShardAggCorrectnessAdObjectXorNw) {
  auto [useTls, usingBatch] = GetParam();
  testForShortCase(
      "kanonymity_allpass",
      true /* XorEncrypted */,
      usingBatch,
      useTls,
      initialPort_ + 10);
}

// -END- AdObject format related test --

// -BEGIN- Test LiftNoNwEncrypt
TEST_P(
    ShardCombinerAppTestFixture,
    TestGenericLiftCorrectnessPlainTextNwEncyption) {
  auto [useTls, usingBatch] = GetParam();
  testLift(false /* XorEncrypted */, usingBatch, useTls, initialPort_ + 20);
}

// Test LiftXor
TEST_P(ShardCombinerAppTestFixture, TestGenericLiftCorrectnessXorNwEncrypted) {
  auto [useTls, usingBatch] = GetParam();
  testLift(true /* XorEncrypted */, usingBatch, useTls, initialPort_ + 30);
}
// -END- Test

INSTANTIATE_TEST_CASE_P(
    ShardCombinerAppTest,
    ShardCombinerAppTestFixture,
    ::testing::Combine(::testing::Bool(), ::testing::Bool()),
    [](const testing::TestParamInfo<ShardCombinerAppTestFixture::ParamType>&
           info) {
      std::string tls = std::get<0>(info.param) ? "UseTls" : "NoTls";
      std::string usingBatch =
          std::get<1>(info.param) ? "UsingBatch" : "NoBatch";
      std::string name = tls + "_" + usingBatch;
      return name;
    });

} // namespace shard_combiner
