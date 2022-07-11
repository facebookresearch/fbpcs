/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <cstdint>
#include <filesystem>
#include <string>
#include <unordered_map>

#include <gtest/gtest.h>
#include <utility>

#include <fbpcf/mpc/EmpGame.h>
#include "folly/Format.h"
#include "folly/Random.h"
#include "folly/dynamic.h"
#include "folly/json.h"
#include "folly/logging/xlog.h"
#include "folly/test/JsonTestUtil.h"

#include "fbpcs/emp_games/attribution/decoupled_attribution/AttributionApp.h"
#include "fbpcs/emp_games/attribution/decoupled_attribution/AttributionMetrics.h"
#include "fbpcs/emp_games/attribution/decoupled_attribution/AttributionOutput.h"
#include "fbpcs/emp_games/attribution/decoupled_attribution/Constants.h"

#include "fbpcf/io/api/FileIOWrappers.h"

namespace aggregation::private_attribution {

using Attribution = folly::dynamic;

template <int PARTY, fbpcf::Visibility OUTPUT_VISIBILITY>
static void runGame(
    const std::string& serverIp,
    const uint16_t port,
    const std::string& attributionRules,
    const std::filesystem::path& inputPath,
    const std::string& outputPath) {
  AttributionApp<PARTY, OUTPUT_VISIBILITY>(
      serverIp, port, attributionRules, inputPath, outputPath)
      .run();
}

// helper function for executing MPC game and generate corresponding output
inline std::pair<AttributionOutputMetrics, AttributionOutputMetrics>
runGameAndGenOutputPUBLIC(
    std::string serverIpAlice,
    int16_t portAlice,
    std::string attributionRuleAlice,
    std::string inputPathAlice,
    std::string outputPathAlice,
    std::string serverIpBob,
    int16_t portBob,
    std::string attributionRuleBob,
    std::string inputPathBob,
    std::string outputPathBob) {
  auto futureAlice = std::async(
      runGame<PUBLISHER, fbpcf::Visibility::Public>,
      serverIpAlice,
      portAlice,
      attributionRuleAlice,
      inputPathAlice,
      outputPathAlice);
  auto futureBob = std::async(
      runGame<PARTNER, fbpcf::Visibility::Public>,
      serverIpBob,
      portBob,
      attributionRuleBob,
      inputPathBob,
      outputPathBob);

  futureAlice.wait();
  futureBob.wait();

  auto resAlice = AttributionOutputMetrics::fromJson(
      fbpcf::io::FileIOWrappers::readFile(outputPathAlice));
  auto resBob = AttributionOutputMetrics::fromJson(
      fbpcf::io::FileIOWrappers::readFile(outputPathBob));

  return std::make_pair(resAlice, resBob);
}

inline std::pair<AttributionOutputMetrics, AttributionOutputMetrics>
runGameAndGenOutputXOR(
    std::string serverIpAlice,
    int16_t portAlice,
    std::string attributionRuleAlice,
    std::string inputPathAlice,
    std::string outputPathAlice,
    std::string serverIpBob,
    int16_t portBob,
    std::string attributionRuleBob,
    std::string inputPathBob,
    std::string outputPathBob) {
  auto futureAlice = std::async(
      runGame<PUBLISHER, fbpcf::Visibility::Xor>,
      serverIpAlice,
      portAlice,
      attributionRuleAlice,
      inputPathAlice,
      outputPathAlice);
  auto futureBob = std::async(
      runGame<PARTNER, fbpcf::Visibility::Xor>,
      serverIpBob,
      portBob,
      attributionRuleBob,
      inputPathBob,
      outputPathBob);

  futureAlice.wait();
  futureBob.wait();

  auto resAlice = AttributionOutputMetrics::fromJson(
      fbpcf::io::FileIOWrappers::readFile(outputPathAlice));
  auto resBob = AttributionOutputMetrics::fromJson(
      fbpcf::io::FileIOWrappers::readFile(outputPathBob));

  return std::make_pair(resAlice, resBob);
}

// verify whether the attribution logic

inline void verifyOutput(
    AttributionOutputMetrics resAlice,
    AttributionOutputMetrics resBob,
    std::string ouputJsonFileName) {
  folly::dynamic expectedOutput =
      folly::parseJson(fbpcf::io::FileIOWrappers::readFile(ouputJsonFileName));

  FOLLY_EXPECT_JSON_EQ(
      folly::toJson(resAlice.toDynamic()), folly::toJson(expectedOutput));
  FOLLY_EXPECT_JSON_EQ(
      folly::toJson(resBob.toDynamic()), folly::toJson(expectedOutput));
}

inline std::pair<AttributionOutputMetrics, AttributionOutputMetrics>
revealXORedResult(
    AttributionOutputMetrics resAlice,
    AttributionOutputMetrics resBob,
    std::string attributionRule) {
  auto aliceAttributionOutput =
      resAlice.ruleToMetrics.at(attributionRule).formatToAttribution;
  auto bobAttributionOutput =
      resBob.ruleToMetrics.at(attributionRule).formatToAttribution;
  auto attributionFormat = "default";

  // initiate new objects to store revealed data
  // use std::move to ensure no memory leak
  folly::dynamic revealedAttributionMetrics = folly::dynamic::object;
  folly::dynamic revealedMetricsMap = folly::dynamic::object;
  folly::dynamic revealedAttributionResultsPerId = folly::dynamic::object;

  // Attribution output contains results based on attribution format (currently
  // only "default").
  AttributionResult aliceAttribution =
      aliceAttributionOutput.at(attributionFormat);
  AttributionResult bobAttribution = bobAttributionOutput.at(attributionFormat);

  // first sort the keys so that alice and bob are reading
  // corresponding rows
  std::vector<std::string> sortedIds;
  for (const auto& id : aliceAttribution.keys()) {
    sortedIds.push_back(id.asString());
  }
  std::sort(sortedIds.begin(), sortedIds.end());

  for (const auto& adId : sortedIds) {
    auto& aliceResults = aliceAttribution.at(adId);
    auto& bobResults = bobAttribution.at(adId);
    folly::dynamic revealedResults = folly::dynamic::array;
    for (auto i = 0; i < aliceResults.size(); i++) {
      const auto& aliceResult =
          OutputMetricDefault::fromDynamic(aliceResults.at(i));
      const auto& bobResult =
          OutputMetricDefault::fromDynamic(bobResults.at(i));
      revealedResults.push_back(OutputMetricDefault{

          aliceResult.is_attributed != bobResult.is_attributed}
                                    .toDynamic());
    }
    revealedAttributionResultsPerId[adId] = revealedResults;
  }
  revealedMetricsMap[attributionFormat] =
      std::move(revealedAttributionResultsPerId);
  revealedAttributionMetrics[attributionRule] = std::move(revealedMetricsMap);

  // return Json format
  return std::make_pair(
      AttributionOutputMetrics::fromDynamic(revealedAttributionMetrics),
      AttributionOutputMetrics::fromDynamic(revealedAttributionMetrics));
}
} // namespace aggregation::private_attribution
