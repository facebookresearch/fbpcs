/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <fbpcf/io/FileManagerUtil.h>
#include <gtest/gtest.h>

#include "folly/dynamic.h"
#include "folly/json.h"
#include "folly/test/JsonTestUtil.h"

#include "fbpcs/emp_games/pcf2_aggregation/AggregationMetrics.h"

namespace pcf2_aggregation {

// verify the aggregation output
inline void verifyOutput(
    AggregationOutputMetrics output,
    std::string outputJsonFileName) {
  folly::dynamic expectedOutput =
      folly::parseJson(fbpcf::io::read(outputJsonFileName));

  FOLLY_EXPECT_JSON_EQ(
      folly::toJson(output.toDynamic()), folly::toJson(expectedOutput));
}

inline AggregationOutputMetrics revealXORedResult(
    AggregationOutputMetrics resAlice,
    AggregationOutputMetrics resBob,
    std::string aggregationFormat,
    std::string attributionRule) {
  auto aliceAggregation = resAlice.ruleToMetrics.at(attributionRule)
                              .formatToAggregation.at(aggregationFormat);
  auto bobAggregation = resBob.ruleToMetrics.at(attributionRule)
                            .formatToAggregation.at(aggregationFormat);

  // initiate new objects to store revealed data
  // use std::move to ensure no memory leak
  folly::dynamic revealedAggregatedMetrics = folly::dynamic::object;
  folly::dynamic revealedMetricsMap = folly::dynamic::object;
  folly::dynamic revealedAggregation = folly::dynamic::object;

  // xor the pairs to reveal the final output
  for (const auto& adId : aliceAggregation.keys()) {
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
    }
  }

  revealedMetricsMap[aggregationFormat] = std::move(revealedAggregation);
  revealedAggregatedMetrics[attributionRule] = std::move(revealedMetricsMap);

  // return Json format
  return AggregationOutputMetrics::fromDynamic(revealedAggregatedMetrics);
}

} // namespace pcf2_aggregation
