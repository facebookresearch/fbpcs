/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <fbpcf/io/api/FileIOWrappers.h>
#include <gtest/gtest.h>

#include "folly/dynamic.h"
#include "folly/json.h"
#include "folly/test/JsonTestUtil.h"

#include "fbpcs/emp_games/pcf2_attribution/AttributionApp.h"
#include "fbpcs/emp_games/pcf2_attribution/AttributionMetrics.h"
#include "fbpcs/emp_games/pcf2_attribution/AttributionOutput.h"
#include "fbpcs/emp_games/pcf2_attribution/Constants.h"

namespace pcf2_attribution {

// verify the attribution output
inline void verifyOutput(
    AttributionOutputMetrics output,
    std::string outputJsonFileName) {
  folly::dynamic expectedOutput =
      folly::parseJson(fbpcf::io::FileIOWrappers::readFile(outputJsonFileName));

  FOLLY_EXPECT_JSON_EQ(
      folly::toJson(output.toDynamic()), folly::toJson(expectedOutput));
}

inline AttributionOutputMetrics revealXORedResult(
    AttributionOutputMetrics resAlice,
    AttributionOutputMetrics resBob,
    std::string attributionRule) {
  auto aliceAttributionOutput = resAlice.ruleToMetrics.at(attributionRule);
  auto bobAttributionOutput = resBob.ruleToMetrics.at(attributionRule);

  auto attributionFormat = "default";

  // initiate new objects to store revealed data
  // use std::move to ensure no memory leak
  folly::dynamic revealedAttributionMetrics = folly::dynamic::object;
  folly::dynamic revealedMetricsMap = folly::dynamic::object;
  folly::dynamic revealedAttributionResultsPerId = folly::dynamic::object;

  AttributionResult aliceAttribution;
  AttributionResult bobAttribution;
  if (FLAGS_use_new_output_format) {
    aliceAttribution = aliceAttributionOutput.attributionResult;
    bobAttribution = bobAttributionOutput.attributionResult;
  } else {
    // Attribution output contains results based on attribution format
    // (currently only "default").
    aliceAttribution =
        aliceAttributionOutput.formatToAttribution.at(attributionFormat);
    bobAttribution =
        bobAttributionOutput.formatToAttribution.at(attributionFormat);
  }

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

  if (FLAGS_use_new_output_format) {
    revealedMetricsMap = std::move(revealedAttributionResultsPerId);

  } else {
    revealedMetricsMap[attributionFormat] =
        std::move(revealedAttributionResultsPerId);
  }
  revealedAttributionMetrics[attributionRule] = std::move(revealedMetricsMap);

  // return Json format
  return AttributionOutputMetrics::fromDynamic(revealedAttributionMetrics);
}

inline AttributionOutputMetrics revealXORedReformattedResult(
    AttributionOutputMetrics resAlice,
    AttributionOutputMetrics resBob,
    std::string attributionRule) {
  auto aliceAttributionOutput = resAlice.ruleToMetrics.at(attributionRule);
  auto bobAttributionOutput = resBob.ruleToMetrics.at(attributionRule);
  auto attributionFormat = "default";

  // initiate new objects to store revealed data
  // use std::move to ensure no memory leak
  folly::dynamic revealedAttributionMetrics = folly::dynamic::object;
  folly::dynamic revealedMetricsMap = folly::dynamic::object;
  folly::dynamic revealedAttributionResultsPerId = folly::dynamic::object;
  AttributionResult aliceAttribution;
  AttributionResult bobAttribution;
  if (FLAGS_use_new_output_format) {
    aliceAttribution = aliceAttributionOutput.attributionResult;
    bobAttribution = bobAttributionOutput.attributionResult;
  } else {
    // Attribution output contains results based on attribution format
    // (currently only "default").
    aliceAttribution =
        aliceAttributionOutput.formatToAttribution.at(attributionFormat);
    bobAttribution =
        bobAttributionOutput.formatToAttribution.at(attributionFormat);
  }

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
          OutputMetricReformatted::fromDynamic(aliceResults.at(i));
      const auto& bobResult =
          OutputMetricReformatted::fromDynamic(bobResults.at(i));

      revealedResults.push_back(OutputMetricReformatted{
          aliceResult.ad_id ^ bobResult.ad_id,
          aliceResult.conv_value ^ bobResult.conv_value,
          aliceResult.is_attributed != bobResult.is_attributed}
                                    .toDynamic());
    }
    revealedAttributionResultsPerId[adId] = revealedResults;
  }
  if (FLAGS_use_new_output_format) {
    revealedMetricsMap = std::move(revealedAttributionResultsPerId);

  } else {
    revealedMetricsMap[attributionFormat] =
        std::move(revealedAttributionResultsPerId);
  }
  revealedAttributionMetrics[attributionRule] = std::move(revealedMetricsMap);

  // return Json format
  return AttributionOutputMetrics::fromDynamic(revealedAttributionMetrics);
}

} // namespace pcf2_attribution
