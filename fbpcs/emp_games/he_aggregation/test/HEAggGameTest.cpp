/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <fbpcf/io/api/FileIOWrappers.h>
#include <gtest/gtest.h>
#include <filesystem>
#include <future>
#include <string>

#include "folly/Format.h"
#include "folly/Random.h"
#include "folly/json.h"
#include "folly/test/JsonTestUtil.h"

#include "folly/logging/xlog.h"

#include "fbpcf/engine/communication/InMemoryPartyCommunicationAgentFactory.h"
#include "fbpcf/engine/communication/test/AgentFactoryCreationHelper.h"
#include "fbpcf/scheduler/PlaintextScheduler.h"
#include "fbpcf/scheduler/SchedulerHelper.h"
#include "fbpcf/test/TestHelper.h"
#include "fbpcs/emp_games/common/TestUtil.h"
#include "fbpcs/emp_games/he_aggregation/AggregationInputMetrics.h"
#include "fbpcs/emp_games/he_aggregation/HEAggApp.h"
#include "fbpcs/emp_games/he_aggregation/HEAggGame.h"
#include "fbpcs/emp_games/he_aggregation/HEAggOptions.h"

namespace pcf2_he {

std::unordered_map<uint64_t, uint64_t> runGame(
    int myId,
    AggregationInputMetrics inputData,
    std::shared_ptr<
        fbpcf::engine::communication::IPartyCommunicationAgentFactory>
        factory) {
  auto game = std::make_unique<HEAggGame>(std::move(factory));
  return game->computeAggregations(myId, std::move(inputData));
}

void verifyOutput(
    const std::unordered_map<uint64_t, uint64_t>& output,
    const std::string& outputJsonFileName) {
  folly::dynamic expectedOutput =
      folly::parseJson(fbpcf::io::FileIOWrappers::readFile(outputJsonFileName));

  folly::dynamic actualOutput = folly::dynamic::object();
  for (auto& [k, v] : output) {
    actualOutput.insert(std::to_string(k), std::to_string(v));
  }

  FOLLY_EXPECT_JSON_EQ(
      folly::toJson(actualOutput), folly::toJson(expectedOutput));
}

TEST(HEAggGameTest, HEAggGameCorrectnessTest) {
  const std::string baseDir_ =
      private_measurement::test_util::getBaseDirFromPath(__FILE__);

  const std::string filePrefix = baseDir_ + "test_correctness/dataset1/";

  // input files
  const std::string publisherClearTextFileName =
      filePrefix + "dataproc_publisher_0.csv";
  const std::string publisherSecretShareFileName =
      filePrefix + "ss_publisher_0.json";

  const std::string partnerClearTextFileName =
      filePrefix + "dataproc_partner_0.csv";
  const std::string partnerSecretShareFileName =
      filePrefix + "ss_partner_0.json";

  // output file
  const std::string outputJsonFileName = filePrefix + "output.json";

  const common::InputEncryption inputEncryption =
      common::InputEncryption::Plaintext;

  // read input files
  AggregationInputMetrics publisherInputData{
      inputEncryption,
      publisherSecretShareFileName,
      publisherClearTextFileName};

  AggregationInputMetrics partnerInputData{
      inputEncryption, partnerSecretShareFileName, partnerClearTextFileName};

  // compute aggregations
  auto factories = fbpcf::engine::communication::getInMemoryAgentFactory(2);

  auto future0 =
      std::async(runGame, 0, publisherInputData, std::move(factories[0]));

  auto future1 =
      std::async(runGame, 1, partnerInputData, std::move(factories[1]));

  auto res0 = future0.get();
  auto res1 = future1.get();

  verifyOutput(res0, outputJsonFileName);
}

} // namespace pcf2_he
