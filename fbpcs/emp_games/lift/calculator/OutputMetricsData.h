/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <sstream>
#include <vector>

#include <folly/String.h>

#include "../common/GroupedLiftMetrics.h"

namespace private_lift {

/*
 * Simple struct representing the metrics in a Lift computation
 */
struct OutputMetricsData {
  int64_t testPopulation = 0;
  int64_t controlPopulation = 0;
  int64_t testEvents = 0;
  int64_t controlEvents = 0;
  int64_t testConverters = 0;
  int64_t controlConverters = 0;
  int64_t testValue = 0;
  int64_t controlValue = 0;
  int64_t testValueSquared = 0;
  int64_t controlValueSquared = 0;
  int64_t testNumConvSquared = 0;
  int64_t controlNumConvSquared = 0;
  int64_t testMatchCount = 0;
  int64_t controlMatchCount = 0;
  int64_t testImpressions = 0;
  int64_t controlImpressions = 0;
  int64_t testClicks = 0;
  int64_t controlClicks = 0;
  int64_t testSpend = 0;
  int64_t controlSpend = 0;
  int64_t testReach = 0;
  int64_t controlReach = 0;
  int64_t testClickers = 0;
  int64_t controlClickers = 0;
  int64_t reachedConversions = 0;
  int64_t reachedValue = 0;
  std::vector<int64_t> testConvHistogram;
  std::vector<int64_t> controlConvHistogram;

  OutputMetricsData() = default;

  OutputMetricsData(bool isConversionLift)
      : isConversionLift_{isConversionLift} {}

  bool isConversionLift() const {
    return isConversionLift_;
  }

  friend std::ostream& operator<<(
      std::ostream& os,
      const OutputMetricsData& out) {
    os << "Test Conversions: " << out.testEvents << "\n";
    os << "Control Conversions: " << out.controlEvents << "\n";
    os << "Test Converters: " << out.testConverters << "\n";
    os << "Control Converters: " << out.controlConverters << "\n";
    os << "Test Value: " << out.testValue << "\n";
    os << "Control Value: " << out.controlValue << "\n";
    os << "Test Value Squared: " << out.testValueSquared << "\n";
    os << "Control Value Squared: " << out.controlValueSquared << "\n";
    os << "Test NumConv Squared: " << out.testNumConvSquared << "\n";
    os << "Control NumConv Squared: " << out.controlNumConvSquared << "\n";
    os << "Test Population: " << out.testPopulation << "\n";
    os << "Control Population: " << out.controlPopulation << "\n";
    os << "Test Match Count: " << out.testMatchCount << "\n";
    os << "Control Match Count: " << out.controlMatchCount << "\n";
    os << "Test Impressions: " << out.testImpressions << "\n";
    os << "Control Impressions: " << out.controlImpressions << "\n";
    os << "Test Clicks: " << out.testClicks << "\n";
    os << "Control Clicks: " << out.controlClicks << "\n";
    os << "Test Spend: " << out.testSpend << "\n";
    os << "Control Spend: " << out.controlSpend << "\n";
    os << "Test Reach: " << out.testReach << "\n";
    os << "Control Reach: " << out.controlReach << "\n";
    os << "Test Clickers: " << out.testClickers << "\n";
    os << "Control Clickers: " << out.controlClickers << "\n";
    os << "Reached Conversions: " << out.reachedConversions << "\n";
    os << "Reached Value: " << out.reachedValue << "\n";
    os << "Test Conversion histogram: " << folly::join(',', out.testConvHistogram) << "\n";
    os << "Control Conversion histogram: " << folly::join(',', out.controlConvHistogram) << "\n";

    return os;
  }

  // Helper method that converts the output metrics of a game implementation
  // to a common lift metrics representation. The LiftMetrics introduced in
  // D22969707 serve as the common metrics data structure between game and
  // aggregator
  LiftMetrics toLiftMetrics() const {
    LiftMetrics metrics{};
    metrics.testPopulation = testPopulation;
    metrics.controlPopulation = controlPopulation;
    metrics.testConversions = testEvents;
    metrics.controlConversions = controlEvents;
    metrics.testConverters = testConverters;
    metrics.controlConverters = controlConverters;
    metrics.testValue = testValue;
    metrics.controlValue = controlValue;
    metrics.testValueSquared = testValueSquared;
    metrics.controlValueSquared = controlValueSquared;
    metrics.testNumConvSquared = testNumConvSquared;
    metrics.controlNumConvSquared = controlNumConvSquared;
    metrics.testMatchCount = testMatchCount;
    metrics.controlMatchCount = controlMatchCount;
    metrics.testImpressions = testImpressions;
    metrics.controlImpressions = controlImpressions;
    metrics.testClicks = testClicks;
    metrics.controlClicks = controlClicks;
    metrics.testSpend = testSpend;
    metrics.controlSpend = controlSpend;
    metrics.testReach = testReach;
    metrics.controlReach = controlReach;
    metrics.testClickers = testClickers;
    metrics.controlClickers = controlClickers;
    metrics.reachedConversions = reachedConversions;
    metrics.reachedValue = reachedValue;
    metrics.testConvHistogram = testConvHistogram;
    metrics.controlConvHistogram = controlConvHistogram;
    return metrics;
  }

 private:
  bool isConversionLift_;
};

} // namespace private_lift
