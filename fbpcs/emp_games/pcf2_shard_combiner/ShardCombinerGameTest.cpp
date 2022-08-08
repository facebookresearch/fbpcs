/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <future>
#include <unordered_map>

#include <gtest/gtest.h>

#include <folly/Format.h>
#include <folly/dynamic.h>
#include <folly/json.h>

#include <fbpcf/engine/communication/SocketPartyCommunicationAgentFactory.h>
#include <fbpcf/engine/communication/test/AgentFactoryCreationHelper.h>
#include <fbpcf/io/api/FileIOWrappers.h>

#include "fbpcs/emp_games/common/Constants.h"
#include "fbpcs/emp_games/common/test/TestUtils.h"
#include "fbpcs/emp_games/pcf2_shard_combiner/AggMetrics.h"
#include "fbpcs/emp_games/pcf2_shard_combiner/AggMetrics_impl.h"
#include "fbpcs/emp_games/pcf2_shard_combiner/ShardCombinerGame.h"
#include "fbpcs/emp_games/pcf2_shard_combiner/ShardValidator.h"

namespace shard_combiner {

template <
    ShardSchemaType shardSchemaType,
    int32_t schedulerId,
    bool usingBatch,
    common::InputEncryption inputEncryption>
std::shared_ptr<ShardCombinerGame<
    shardSchemaType,
    schedulerId,
    usingBatch,
    inputEncryption>>
getGameInstance(
    std::shared_ptr<
        fbpcf::engine::communication::IPartyCommunicationAgentFactory> factory,
    fbpcf::SchedulerCreator schedulerCreator) {
  auto scheduler = schedulerCreator(schedulerId, *factory);

  return std::make_shared<ShardCombinerGame<
      shardSchemaType,
      schedulerId,
      usingBatch,
      inputEncryption>>(std::move(scheduler), std::move(factory), 1);
}

// returns a map of revealed folly::dynamic objects indexed by schedulerId.
// where schedulerId = 0 -> Publisher common::PUBLISHER
//       schedulerId = 1 -> Partner common::PARTNER
template <
    ShardSchemaType shardSchemaType,
    int32_t schedulerId,
    bool usingBatch,
    common::InputEncryption inputEncryption>
std::unordered_map<int32_t, folly::dynamic> runGameTest(
    std::string inputDir,
    std::string filename,
    int32_t numShards,
    std::shared_ptr<
        fbpcf::engine::communication::IPartyCommunicationAgentFactory> factory,
    fbpcf::SchedulerCreator schedulerCreator) {
  auto game = getGameInstance<
      shardSchemaType,
      schedulerId,
      usingBatch,
      inputEncryption>(factory, schedulerCreator);
  auto new_metrics = game->readShards(inputDir, filename, numShards);
  auto res = game->play(new_metrics);

  std::unordered_map<int32_t, folly::dynamic> ret;
  ret.insert(std::make_pair(
      common::PUBLISHER, res->toRevealedDynamic(common::PUBLISHER)));
  ret.insert(
      std::make_pair(common::PARTNER, res->toRevealedDynamic(common::PARTNER)));

  return ret;
}

template <
    bool usingBatch,
    ShardSchemaType shardSchemaType = ShardSchemaType::kTest>
void runTestWithParams(
    common::SchedulerType schedulerType,
    std::string baseDir,
    std::string partnerFileName,
    std::string publisherFileName,
    int32_t numShards,
    std::string expectedOutFileName) {
  constexpr common::InputEncryption inputEncryption =
      common::InputEncryption::Xor;

  auto factories = fbpcf::engine::communication::getInMemoryAgentFactory(2);

  fbpcf::SchedulerCreator schedulerCreator =
      fbpcf::getSchedulerCreator<true>(schedulerType);

  auto gamePartner = std::async(
      std::launch::async,
      runGameTest<
          shardSchemaType,
          common::PARTNER,
          usingBatch,
          inputEncryption>,
      baseDir,
      partnerFileName,
      numShards,
      std::move(factories[common::PARTNER]),
      schedulerCreator);

  auto gamePublisher = std::async(
      std::launch::async,
      runGameTest<
          shardSchemaType,
          common::PUBLISHER,
          usingBatch,
          inputEncryption>,
      baseDir,
      publisherFileName,
      numShards,
      std::move(factories[common::PUBLISHER]),
      schedulerCreator);

  auto f1 = gamePartner.get();
  auto f2 = gamePublisher.get();

  auto expectedObj = folly::parseJson(
      fbpcf::io::FileIOWrappers::readFile(baseDir + expectedOutFileName));

  EXPECT_EQ(f1.at(common::PARTNER), expectedObj);
  EXPECT_EQ(f2.at(common::PUBLISHER), expectedObj);
}

template <
    ShardSchemaType shardSchemaType,
    int32_t schedulerId,
    bool usingBatch,
    common::InputEncryption inputEncryption>
std::unordered_map<std::pair<int32_t, int32_t>, folly::dynamic> readFileInGame(
    std::string inputDir,
    std::string filename,
    int32_t numShards,
    std::shared_ptr<
        fbpcf::engine::communication::IPartyCommunicationAgentFactory> factory,
    fbpcf::SchedulerCreator schedulerCreator) {
  auto game = getGameInstance<
      shardSchemaType,
      schedulerId,
      usingBatch,
      inputEncryption>(factory, schedulerCreator);
  auto new_metrics = game->readShards(inputDir, filename, numShards);

  std::unordered_map<std::pair<int32_t, int32_t>, folly::dynamic> ret;
  for (size_t i = 0; i < new_metrics.size(); i++) {
    auto res = new_metrics.at(i);
    ret.insert(std::make_pair(
        std::make_pair(i, common::PUBLISHER),
        res->toRevealedDynamic(common::PUBLISHER)));
    ret.insert(std::make_pair(
        std::make_pair(i, common::PARTNER),
        res->toRevealedDynamic(common::PARTNER)));
  }
  return ret;
}

template <bool usingBatch>
void runTestReadFiles(
    common::SchedulerType schedulerType,
    std::string baseDir,
    std::string partnerFileName,
    std::string publisherFileName,
    int32_t numShards,
    std::string expectedOutFileName) {
  constexpr common::InputEncryption inputEncryption =
      common::InputEncryption::Xor;

  auto factories = fbpcf::engine::communication::getInMemoryAgentFactory(2);

  fbpcf::SchedulerCreator schedulerCreator =
      fbpcf::getSchedulerCreator<true>(schedulerType);

  auto gamePartner = std::async(
      std::launch::async,
      readFileInGame<
          ShardSchemaType::kTest,
          common::PARTNER,
          usingBatch,
          inputEncryption>,
      baseDir,
      partnerFileName,
      numShards,
      std::move(factories[common::PARTNER]),
      schedulerCreator);

  auto gamePublisher = std::async(
      std::launch::async,
      readFileInGame<
          ShardSchemaType::kTest,
          common::PUBLISHER,
          usingBatch,
          inputEncryption>,
      baseDir,
      publisherFileName,
      numShards,
      std::move(factories[common::PUBLISHER]),
      schedulerCreator);

  auto f1 = gamePartner.get();
  auto f2 = gamePublisher.get();
  auto testFn =
      [expectedOutFileName, baseDir](
          const std::unordered_map<std::pair<int32_t, int32_t>, folly::dynamic>&
              obj,
          int32_t party) {
        for (const auto& [kp, v] : obj) {
          if (kp.second == party) {
            std::string expectedShardFilePath = folly::sformat(
                "{}/{}_{}", baseDir, expectedOutFileName, kp.first);
            auto expectedObj = folly::parseJson(
                fbpcf::io::FileIOWrappers::readFile(expectedShardFilePath));
            EXPECT_EQ(v, expectedObj);
          }
          std::cout << "f <" << kp.first << "><" << kp.second << ">: " << v
                    << std::endl;
        }
      };
  testFn(f1, common::PARTNER);
  testFn(f2, common::PUBLISHER);
}

class ShardCombinerGameTestFixture
    : public ::testing::TestWithParam<std::tuple<common::SchedulerType, bool>> {
 protected:
  void SetUp() override {
    std::string filePath = __FILE__;
    baseDir_ = filePath.substr(0, filePath.rfind("/")) + "/test/";
  }

  std::string baseDir_;
};

// This test checks if the combiner logic works as expected.
// There are 2 main tests,
//    1. Add 2 shards (even number of shards): 100+90 == 190? "PASS":"FAIL"
//    2. Add 3 shards (odd number of shards): 100+90+10 == 200? "PASS":"FAIL"
TEST_P(ShardCombinerGameTestFixture, TestAggLogic) {
  auto [schedulerType, usingBatch] = GetParam();
  std::string partnerFileName = "input_partner.json";
  std::string publisherFileName = "input_publisher.json";
  std::string expectedOutFileNamePrefix = "expected_out_shards_";
  auto testFn = [&](int32_t numShards,
                    bool usingBatch,
                    common::SchedulerType schedulerType) {
    std::string expectedOutFileName =
        folly::sformat("{}{}.json", expectedOutFileNamePrefix, numShards);
    if (usingBatch) {
      runTestWithParams<true>(
          schedulerType,
          baseDir_ + "combiner_logic_test/",
          partnerFileName,
          publisherFileName,
          numShards,
          expectedOutFileName);
    } else {
      runTestWithParams<false>(
          schedulerType,
          baseDir_ + "combiner_logic_test/",
          partnerFileName,
          publisherFileName,
          numShards,
          expectedOutFileName);
    }
  };

  testFn(2, usingBatch, schedulerType);
  testFn(3, usingBatch, schedulerType);
}

// This test checks if 2 shards that have different attribution
// measurement keys can be combined correctly.
TEST_P(ShardCombinerGameTestFixture, TestAggAdObj) {
  auto [schedulerType, usingBatch] = GetParam();
  std::string partnerFileName = "partner_attribution_out.json";
  std::string publisherFileName = "publisher_attribution_out.json";
  std::string expectedOutFileName = "expected_attribution_out.json";

  if (usingBatch) {
    runTestWithParams<true>(
        schedulerType,
        baseDir_ + "ad_object_format/",
        partnerFileName,
        publisherFileName,
        2,
        expectedOutFileName);
  } else {
    runTestWithParams<false>(
        schedulerType,
        baseDir_ + "ad_object_format/",
        partnerFileName,
        publisherFileName,
        2,
        expectedOutFileName);
  }
}

// This test checks if the AggMetrics is populated correctly
// and verifies by opening/revealing the data to party.
TEST_P(ShardCombinerGameTestFixture, TestReadOpenToParty) {
  auto [schedulerType, usingBatch] = GetParam();
  std::string partnerFileName = "partner_attribution_out.json";
  std::string publisherFileName = "publisher_attribution_out.json";
  std::string expectedOutFileName = "plaintext_attribution_out.json";
  if (usingBatch) {
    runTestReadFiles<true>(
        schedulerType,
        baseDir_ + "ad_object_format/",
        partnerFileName,
        publisherFileName,
        2,
        expectedOutFileName);
  } else {
    runTestReadFiles<false>(
        schedulerType,
        baseDir_ + "ad_object_format/",
        partnerFileName,
        publisherFileName,
        2,
        expectedOutFileName);
  }
}

// This test verifies if the threshold checker works or not.
// tests on odd and even number of shards.
TEST_P(ShardCombinerGameTestFixture, TestThresholdChecker) {
  auto [schedulerType, usingBatch] = GetParam();
  std::string partnerFileName = "partner_lift_input_shard.json";
  std::string publisherFileName = "publisher_lift_input_shard.json";
  std::string expectedOutFileNamePrefix = "lift_expected_output_shards_";
  auto testFn = [&](int32_t numShards,
                    bool usingBatch,
                    common::SchedulerType schedulerType) {
    std::string expectedOutFileName =
        folly::sformat("{}{}.json", expectedOutFileNamePrefix, numShards);
    if (usingBatch) {
      runTestWithParams<true, ShardSchemaType::kGroupedLiftMetrics>(
          schedulerType,
          baseDir_ + "lift_threshold_test/",
          partnerFileName,
          publisherFileName,
          numShards,
          expectedOutFileName);
    } else {
      runTestWithParams<false, ShardSchemaType::kGroupedLiftMetrics>(
          schedulerType,
          baseDir_ + "lift_threshold_test/",
          partnerFileName,
          publisherFileName,
          numShards,
          expectedOutFileName);
    }
  };
  testFn(2, usingBatch, schedulerType);
  testFn(3, usingBatch, schedulerType);
}

INSTANTIATE_TEST_SUITE_P(
    ShardCombinerGameTest,
    ShardCombinerGameTestFixture,
    ::testing::Combine(
        ::testing::Values(
            common::SchedulerType::NetworkPlaintext,
            common::SchedulerType::Eager,
            common::SchedulerType::Lazy),
        ::testing::Values(true, false)),
    [](const testing::TestParamInfo<ShardCombinerGameTestFixture::ParamType>&
           info) {
      auto schedulerType = std::get<0>(info.param);
      auto usingBatch = std::get<1>(info.param);

      return fbpcf::getSchedulerName(schedulerType) +
          (usingBatch ? "Batch" : "NotBatch");
    });

} // namespace shard_combiner
