/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <filesystem>
#include <memory>
#include <thread>
#include <vector>

#include <folly/Random.h>
#include <folly/test/JsonTestUtil.h>
#include <gtest/gtest.h>

#include <fbpcf/io/api/FileIOWrappers.h>
#include <fbpcf/mpc/EmpGame.h>
#include "../../common/TestUtil.h"
#include "AggMetrics.h"
#include "ShardAggregatorApp.h"

namespace measurement::private_attribution {

class ShardAggregatorAppTest : public ::testing::Test {
 protected:
  void SetUp() override {
    port_ = 5000 + folly::Random::rand32() % 1000;

    baseDir_ =
        private_measurement::test_util::getBaseDirFromPath(__FILE__) + "test/";
    std::string tempDir = std::filesystem::temp_directory_path();
    outputPathAlice_ = folly::sformat(
        "{}/output_path_alice.json_{}", tempDir, folly::Random::secureRand64());
    outputPathBob_ = folly::sformat(
        "{}/output_path_bob.json_{}", tempDir, folly::Random::secureRand64());
  }

  void TearDown() override {
    std::filesystem::remove(outputPathAlice_);
    std::filesystem::remove(outputPathBob_);
  }

  static void runGame(
      fbpcf::Party party,
      fbpcf::Visibility visibility,
      const std::string& serverIp,
      uint16_t port,
      int32_t firstShardIndex,
      int32_t numShards,
      int64_t threshold,
      const std::string& inputPath,
      const std::string& outputPath,
      const std::string& inputMappingPath,
      const bool useNewOutputFormat,
      const std::string& metricsFormatType) {
    ShardAggregatorApp(
        party,
        visibility,
        serverIp,
        port,
        firstShardIndex,
        numShards,
        threshold,
        inputPath,
        outputPath,
        inputMappingPath,
        useNewOutputFormat,
        metricsFormatType)
        .run();
  }

  void runAppTest(
      int32_t numShards,
      int64_t threshold,
      const std::string& inputPathAlice,
      const std::string& inputPathBob,
      const std::string& metricsFormatType,
      const std::string& expectedAliceOutPath,
      const std::string& expectedBobOutPath,
      const std::string& inputMappingPath,
      const bool useNewOutputFormat,
      const fbpcf::Visibility visibility = fbpcf::Visibility::Public) {
    auto futureAlice = std::async(
        runGame,
        fbpcf::Party::Alice,
        visibility,
        "", // serverIp
        port_,
        0, // firstShardIndex
        numShards,
        threshold,
        inputPathAlice,
        outputPathAlice_,
        inputMappingPath,
        useNewOutputFormat,
        metricsFormatType);
    auto futureBob = std::async(
        runGame,
        fbpcf::Party::Bob,
        visibility,
        "127.0.0.1",
        port_,
        0, // firstShardIndex
        numShards,
        threshold,
        inputPathBob,
        outputPathBob_,
        inputMappingPath,
        useNewOutputFormat,
        metricsFormatType);

    futureAlice.wait();
    futureBob.wait();

    auto resAlice =
        folly::parseJson(fbpcf::io::FileIOWrappers::readFile(outputPathAlice_));
    auto resBob =
        folly::parseJson(fbpcf::io::FileIOWrappers::readFile(outputPathBob_));

    folly::dynamic expectedOutAlice = folly::parseJson(
        fbpcf::io::FileIOWrappers::readFile(expectedAliceOutPath));
    folly::dynamic expectedOutBob = folly::parseJson(
        fbpcf::io::FileIOWrappers::readFile(expectedBobOutPath));

    FOLLY_EXPECT_JSON_EQ(
        folly::toJson(resAlice), folly::toJson(expectedOutAlice));
    FOLLY_EXPECT_JSON_EQ(folly::toJson(resBob), folly::toJson(expectedOutBob));
  }

  uint16_t port_;
  std::string baseDir_;
  std::string outputPathAlice_;
  std::string outputPathBob_;
};

// Test cases are iterate in https://fb.quip.com/IUHDApxKEAli
TEST_F(ShardAggregatorAppTest, TestGenericShardAggCorrectnessAdObject) {
  std::vector<std::string> caseShortNames = {
      "old",
      "mmt_nooverlap",
      "mmt_overlap",
      "clickonly_touchonly",
      "clicktouch_touchonly",
      "clickonly_clicktouch",
      "clicktouch_clicktouch",
      "kanonymity_mix",
      "kanonymity_allpass",
      "kanonymity_allfail"};
  for (int i = 0; i < caseShortNames.size(); ++i) {
    auto caseShortName = caseShortNames[i];
    const std::string inputPathAlice = baseDir_ +
        "ad_object_format/publisher_attribution_correctness_" + caseShortName +
        "_out.json";
    const std::string inputPathBob = baseDir_ +
        "ad_object_format/partner_attribution_correctness_" + caseShortName +
        "_out.json";
    const std::string expectedOutPath = baseDir_ +
        "expected_shard_aggregator_correctness_test/expected_shard_aggregator_correctness_" +
        caseShortName + "_out.json";

    // For normal tests, set kanonymity threshold to zero
    auto kanonymityThreshold = 0;
    // For k-anonymity dedicated tests, set threshold to 100
    if (caseShortName.find("kanonymity") != std::string::npos) {
      kanonymityThreshold = 100;
    }
    runAppTest(
        2, // numShards
        kanonymityThreshold,
        inputPathAlice,
        inputPathBob,
        "ad_object", // metricsFormatType
        expectedOutPath,
        expectedOutPath,
        "",
        false);
  }
}

TEST_F(ShardAggregatorAppTest, TestGenericShardAggSimpleAdObject) {
  const std::string inputPathAlice =
      baseDir_ + "ad_object_format/publisher_attribution_out.json";
  const std::string inputPathBob =
      baseDir_ + "ad_object_format/partner_attribution_out.json";
  const std::string expectedOutPath = baseDir_ +
      "expected_shard_aggregator_correctness_test/expected_shard_aggregator_out.json";

  runAppTest(
      2, // numShards
      100, // kanonymityThreshold
      inputPathAlice,
      inputPathBob,
      "ad_object", // metricsFormatType
      expectedOutPath,
      expectedOutPath,
      "",
      false);
}

TEST_F(ShardAggregatorAppTest, TestGenericShardAggCorrectnessLift) {
  const std::string liftDir = "lift/";
  const std::string inputPathAlice = baseDir_ + liftDir + "aggregator_alice";
  const std::string inputPathBob = baseDir_ + liftDir + "aggregator_bob";
  const std::string expectedOutPath = baseDir_ + liftDir + "aggregator_metrics";

  auto kanonymityThreshold = 0;
  runAppTest(
      3, // numShards
      kanonymityThreshold,
      inputPathAlice,
      inputPathBob,
      "lift", // metricsFormatType
      expectedOutPath,
      expectedOutPath,
      "",
      false);
}

TEST_F(
    ShardAggregatorAppTest,
    TestGenericShardAggCorrectnessLiftVisibilityPublic) {
  const std::string liftDir = "lift/";
  const std::string inputPathAlice = baseDir_ + liftDir + "aggregator_alice";
  const std::string inputPathBob = baseDir_ + liftDir + "aggregator_bob";
  const std::string expectedOutPath =
      baseDir_ + liftDir + "aggregator_metrics_kanon";

  auto kanonymityThreshold = 100;
  runAppTest(
      3, // numShards
      kanonymityThreshold,
      inputPathAlice,
      inputPathBob,
      "lift", // metricsFormatType
      expectedOutPath,
      expectedOutPath,
      "",
      false);
}

TEST_F(
    ShardAggregatorAppTest,
    TestGenericShardAggCorrectnessLiftVisibilityBob) {
  const std::string liftDir = "lift/";
  const std::string inputPathAlice = baseDir_ + liftDir + "aggregator_alice";
  const std::string inputPathBob = baseDir_ + liftDir + "aggregator_bob";
  const std::string expectedOutPath =
      baseDir_ + liftDir + "aggregator_metrics_kanon";
  const std::string zeroMetrics = baseDir_ + liftDir + "zero_metrics";

  auto kanonymityThreshold = 50;
  runAppTest(
      3, // numShards
      kanonymityThreshold,
      inputPathAlice,
      inputPathBob,
      "lift", // metricsFormatType
      zeroMetrics,
      expectedOutPath,
      "",
      false,
      fbpcf::Visibility::Bob);
}

TEST_F(ShardAggregatorAppTest, TestGenericShardAggCorrectnessLiftAnonymous) {
  const std::string liftDir = "lift/";
  const std::string inputPathAlice = baseDir_ + liftDir + "aggregator_alice";
  const std::string inputPathBob = baseDir_ + liftDir + "aggregator_bob";
  const std::string expectedOutPath =
      baseDir_ + liftDir + "aggregator_metrics_kanon_anonymous";

  auto kanonymityThreshold = std::numeric_limits<int64_t>::max();
  runAppTest(
      3, // numShards
      kanonymityThreshold,
      inputPathAlice,
      inputPathBob,
      "lift", // metricsFormatType
      expectedOutPath,
      expectedOutPath,
      "",
      false);
}
} // namespace measurement::private_attribution
