/*
 * Copyright (c) Facebook, Inc. and its affiliates.
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

#include "../AttributionApp.h"
#include "../AttributionMetrics.h"

#include <fbpcf/io/FileManagerUtil.h>

namespace measurement::private_attribution {

// helper function for runGame

template <int PARTY, fbpcf::Visibility OUTPUT_VISIBILITY>
static void runGame(
    const std::string& serverIp,
    const uint16_t port,
    const std::string& attributionRules,
    const std::string& aggregators,
    const std::filesystem::path& inputPath,
    const std::string& outputPath) {
  AttributionApp<PARTY, OUTPUT_VISIBILITY>(
      serverIp, port, attributionRules, aggregators, inputPath, outputPath)
      .run();
}

// helper function for executing MPC game and generate corresponding output
inline std::pair<AttributionOutputMetrics, AttributionOutputMetrics>
runGameAndGenOutputPUBLIC(
    std::string serverIpAlice,
    int16_t portAlice,
    std::string attributionRuleAlice,
    std::string aggregatorAlice,
    std::string inputPathAlice,
    std::string outputPathAlice,
    std::string serverIpBob,
    int16_t portBob,
    std::string attributionRuleBob,
    std::string aggregatorBob,
    std::string inputPathBob,
    std::string outputPathBob) {
  auto futureAlice = std::async(
      runGame<PUBLISHER, fbpcf::Visibility::Public>,
      serverIpAlice,
      portAlice,
      attributionRuleAlice,
      aggregatorAlice,
      inputPathAlice,
      outputPathAlice);
  auto futureBob = std::async(
      runGame<PARTNER, fbpcf::Visibility::Public>,
      serverIpBob,
      portBob,
      attributionRuleBob,
      aggregatorBob,
      inputPathBob,
      outputPathBob);

  futureAlice.wait();
  futureBob.wait();

  auto resAlice =
      AttributionOutputMetrics::fromJson(fbpcf::io::read(outputPathAlice));
  auto resBob =
      AttributionOutputMetrics::fromJson(fbpcf::io::read(outputPathBob));

  return std::make_pair(resAlice, resBob);
}

inline std::pair<AttributionOutputMetrics, AttributionOutputMetrics>
runGameAndGenOutputXOR(
    std::string serverIpAlice,
    int16_t portAlice,
    std::string attributionRuleAlice,
    std::string aggregatorAlice,
    std::string inputPathAlice,
    std::string outputPathAlice,
    std::string serverIpBob,
    int16_t portBob,
    std::string attributionRuleBob,
    std::string aggregatorBob,
    std::string inputPathBob,
    std::string outputPathBob) {
  auto futureAlice = std::async(
      runGame<PUBLISHER, fbpcf::Visibility::Xor>,
      serverIpAlice,
      portAlice,
      attributionRuleAlice,
      aggregatorAlice,
      inputPathAlice,
      outputPathAlice);
  auto futureBob = std::async(
      runGame<PARTNER, fbpcf::Visibility::Xor>,
      serverIpBob,
      portBob,
      attributionRuleBob,
      aggregatorBob,
      inputPathBob,
      outputPathBob);

  futureAlice.wait();
  futureBob.wait();

  auto resAlice =
      AttributionOutputMetrics::fromJson(fbpcf::io::read(outputPathAlice));
  auto resBob =
      AttributionOutputMetrics::fromJson(fbpcf::io::read(outputPathBob));

  return std::make_pair(resAlice, resBob);
}

// verify whether the attribution logic

inline void verifyOutput(
    AttributionOutputMetrics resAlice,
    AttributionOutputMetrics resBob,
    std::string ouputJsonFileName) {
  folly::dynamic expectedOutput =
      folly::parseJson(fbpcf::io::read(ouputJsonFileName));

  FOLLY_EXPECT_JSON_EQ(
      folly::toJson(resAlice.toDynamic()), folly::toJson(expectedOutput));
  FOLLY_EXPECT_JSON_EQ(
      folly::toJson(resBob.toDynamic()), folly::toJson(expectedOutput));
}

inline std::pair<AttributionOutputMetrics, AttributionOutputMetrics>
revealXORedResult(
    AttributionOutputMetrics resAlice,
    AttributionOutputMetrics resBob,
    std::string aggregator,
    std::string attributionRule) {
  auto aliceAggregationOutput =
      resAlice.ruleToMetrics.at(attributionRule).formatToAggregation;
  auto bobAggregationOutput =
      resBob.ruleToMetrics.at(attributionRule).formatToAggregation;

  Aggregation aliceAggregation = aliceAggregationOutput.at(aggregator);
  Aggregation bobAggregation = bobAggregationOutput.at(aggregator);
  // initiate new objects to store revealed data
  // use std::move to ensure no memory leak
  folly::dynamic revealedAggregatedMetrics = folly::dynamic::object;
  folly::dynamic revealedMetricsMap = folly::dynamic::object;
  folly::dynamic revealedAggregation = folly::dynamic::object;

  // first sort the keys so that alice and bob are reading
  // corresponding rows
  std::vector<std::string> sortedAdIds;
  for (const auto& id : aliceAggregation.keys()) {
    sortedAdIds.push_back(id.asString());
  }
  std::sort(sortedAdIds.begin(), sortedAdIds.end());
  // now xor the alice/bob pairs to reveal the final output
  for (const auto& adId : sortedAdIds) {
    if (aggregator == "measurement") {
      // reveal conv metric
      ConvMetrics aliceConvMetrics =
          ConvMetrics::fromDynamic(aliceAggregation.at(adId));
      ConvMetrics bobConvMetrics =
          ConvMetrics::fromDynamic(bobAggregation.at(adId));
      ConvMetrics convMetrics = ConvMetrics{
          aliceConvMetrics.convs ^ bobConvMetrics.convs,
          aliceConvMetrics.sales ^ bobConvMetrics.sales};

      revealedAggregation[adId] = convMetrics.toDynamic();
    } else if (aggregator == "attribution") {
      folly::dynamic metricsList = folly::dynamic::object;

      // soring impIds in Alice
      std::vector<int64_t> aliceSortedImpIds;
      for (const auto& id : aliceAggregation.at(adId).keys()) {
        aliceSortedImpIds.push_back(id.asInt());
      }
      std::sort(aliceSortedImpIds.begin(), aliceSortedImpIds.end());
      // populating alinePairs based on sorted impId
      std::vector<std::pair<int64_t, AemConvMetric>> alicePairs;
      for (const auto& impId : aliceSortedImpIds) {
        AemConvMetric aliceMetrics = AemConvMetric::fromDynamic(
            aliceAggregation.at(adId)[std::to_string(impId)]);
        alicePairs.push_back(std::make_pair(impId, aliceMetrics));
      }

      // soring impIds in Bob
      std::vector<int64_t> bobSortedImpIds;
      for (const auto& id : bobAggregation.at(adId).keys()) {
        bobSortedImpIds.push_back(id.asInt());
      }
      std::sort(bobSortedImpIds.begin(), bobSortedImpIds.end());
      // populating bobPairs based on sorted impId
      std::vector<std::pair<int64_t, AemConvMetric>> bobPairs;
      for (const auto& impId : bobSortedImpIds) {
        AemConvMetric bobMetrics = AemConvMetric::fromDynamic(
            bobAggregation.at(adId)[std::to_string(impId)]);
        bobPairs.push_back(std::make_pair(impId, bobMetrics));
      }

      CHECK_EQ(alicePairs.size(), bobPairs.size())
          << "Publisher and partner's vectors are not the same length.";

      for (auto i = 0; i < alicePairs.size(); i++) {
        AemConvMetric metric = AemConvMetric{};

        auto impId = alicePairs[i].first ^ bobPairs[i].first;
        metric.campaign_bits = alicePairs[i].second.campaign_bits ^
            bobPairs[i].second.campaign_bits;
        for (auto j = 0; j < alicePairs[i].second.conversion_bits.size(); j++) {
          metric.conversion_bits.push_back(
              alicePairs[i].second.conversion_bits[j] ^
              bobPairs[i].second.conversion_bits[j]);
        }
        for (auto j = 0; j < alicePairs[i].second.is_attributed.size(); j++) {
          metric.is_attributed.push_back(
              alicePairs[i].second.is_attributed[j] ^
              bobPairs[i].second.is_attributed[j]);
        }
        metricsList[std::to_string(impId)] = metric.toDynamic();
      }
      revealedAggregation[adId] = metricsList;
    } else {
      throw std::runtime_error(folly::sformat(
          "Unsupported aggregationName: [{}] passed to Shard Aggregator",
          aggregator));
      exit(1);
    }
  }

  // now, execute std::move
  revealedMetricsMap[aggregator] = std::move(revealedAggregation);
  revealedAggregatedMetrics[attributionRule] = std::move(revealedMetricsMap);

  // return AttributionOutputMetrics::fromDynamic(revealedAggregatedMetrics);
  // return Json format
  return std::make_pair(
      AttributionOutputMetrics::fromDynamic(revealedAggregatedMetrics),
      AttributionOutputMetrics::fromDynamic(revealedAggregatedMetrics));
}

} // namespace measurement::private_attribution
