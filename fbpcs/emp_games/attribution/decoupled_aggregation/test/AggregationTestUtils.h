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

#include "fbpcs/emp_games/attribution/decoupled_aggregation/AggregationApp.h"
#include "fbpcs/emp_games/attribution/decoupled_aggregation/AggregationMetrics.h"
#include "fbpcs/emp_games/attribution/decoupled_aggregation/Constants.h"

namespace aggregation::private_aggregation {

template <int PARTY, fbpcf::Visibility OUTPUT_VISIBILITY>
static void runGame(
    const std::string& serverIp,
    const uint16_t port,
    const std::string& aggregationFormat,
    const std::string& inputSecretShareFilePath,
    const std::string& inputClearTextFilePath,
    const std::string& outputPath) {
  AggregationApp<PARTY, OUTPUT_VISIBILITY>(
      serverIp,
      port,
      aggregationFormat,
      inputSecretShareFilePath,
      inputClearTextFilePath,
      outputPath,
      false, // useTls
      "") // tlsDir
      .run();
}

template <fbpcf::Visibility OUTPUT_VISIBILITY>
inline std::pair<AggregationOutputMetrics, AggregationOutputMetrics>
runGameAndGenOutput(
    std::string serverIpAlice,
    int16_t portAlice,
    std::string aggregationFormatAlice,
    std::string inputSecretShareFilePathAlice,
    std::string inputClearTextFilePathAlice,
    std::string outputPathAlice,
    std::string serverIpBob,
    int16_t portBob,
    std::string aggregationFormatBob,
    std::string inputSecretShareFilePathBob,
    std::string inputClearTextFilePathBob,
    std::string outputPathBob) {
  auto futureAlice = std::async(
      runGame<PUBLISHER, OUTPUT_VISIBILITY>,
      serverIpAlice,
      portAlice,
      aggregationFormatAlice,
      inputSecretShareFilePathAlice,
      inputClearTextFilePathAlice,
      outputPathAlice);
  auto futureBob = std::async(
      runGame<PARTNER, OUTPUT_VISIBILITY>,
      serverIpBob,
      portBob,
      aggregationFormatBob,
      inputSecretShareFilePathBob,
      inputClearTextFilePathBob,
      outputPathBob);

  futureAlice.wait();
  futureBob.wait();

  auto resAlice =
      AggregationOutputMetrics::fromJson(fbpcf::io::read(outputPathAlice));
  auto resBob =
      AggregationOutputMetrics::fromJson(fbpcf::io::read(outputPathBob));

  return std::make_pair(resAlice, resBob);
}

// verify the revealed actual aggregation output with expected.
inline void verifyOutput(
    AggregationOutputMetrics resAlice,
    AggregationOutputMetrics resBob,
    std::string ouputJsonFileName) {
  folly::dynamic expectedOutput =
      folly::parseJson(fbpcf::io::read(ouputJsonFileName));

  FOLLY_EXPECT_JSON_EQ(
      folly::toJson(resAlice.toDynamic()), folly::toJson(expectedOutput));
  FOLLY_EXPECT_JSON_EQ(
      folly::toJson(resBob.toDynamic()), folly::toJson(expectedOutput));
}

inline std::pair<AggregationOutputMetrics, AggregationOutputMetrics>
revealXORedResult(
    AggregationOutputMetrics& resAlice,
    AggregationOutputMetrics& resBob,
    std::string& aggregationFormat,
    std::string& attributionRule) {
  auto aliceAggregationOutput = resAlice.ruleToMetrics.at(attributionRule);
  auto bobAggregationOutput = resBob.ruleToMetrics.at(attributionRule);

  // initiate new objects to store revealed data
  // use std::move to ensure no memory leak
  folly::dynamic revealedAggregatedMetrics = folly::dynamic::object;
  folly::dynamic revealedMetricsMap = folly::dynamic::object;
  folly::dynamic revealedAggregation = folly::dynamic::object;

  // Attribution output contains results based on attribution format (currently
  // only "default").
  auto aliceAggregationResults = aliceAggregationOutput.formatToAggregation;
  auto bobAggregationResults = bobAggregationOutput.formatToAggregation;

  auto aliceAggregation = aliceAggregationResults.at(aggregationFormat);
  auto bobAggregation = bobAggregationResults.at(aggregationFormat);

  // first sort the keys so that alice and bob are reading
  // corresponding rows
  std::vector<std::string> sortedAdIds;
  for (const auto& id : aliceAggregation.keys()) {
    sortedAdIds.push_back(id.asString());
  }
  std::sort(sortedAdIds.begin(), sortedAdIds.end());
  // now xor the alice/bob pairs to reveal the final output
  for (const auto& adId : sortedAdIds) {
    if (aggregationFormat == "measurement") {
      // reveal conv metric
      ConvMetrics aliceConvMetrics =
          ConvMetrics::fromDynamic(aliceAggregation.at(adId));
      ConvMetrics bobConvMetrics =
          ConvMetrics::fromDynamic(bobAggregation.at(adId));
      ConvMetrics convMetrics = ConvMetrics{
          aliceConvMetrics.convs ^ bobConvMetrics.convs,
          aliceConvMetrics.sales ^ bobConvMetrics.sales};

      revealedAggregation[adId] = convMetrics.toDynamic();
    } else {
      throw std::runtime_error(folly::sformat(
          "Unsupported aggregationName: [{}] passed to Aggregation correctness test.",
          aggregationFormat));
      exit(1);
    }
  }

  // now, execute std::move
  revealedMetricsMap[aggregationFormat] = std::move(revealedAggregation);
  revealedAggregatedMetrics[attributionRule] = std::move(revealedMetricsMap);

  // return AttributionOutputMetrics::fromDynamic(revealedAggregatedMetrics);
  // return Json format
  return std::make_pair(
      AggregationOutputMetrics::fromDynamic(revealedAggregatedMetrics),
      AggregationOutputMetrics::fromDynamic(revealedAggregatedMetrics));
}
} // namespace aggregation::private_aggregation
