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
    os << "Test Match Count: " << out.testMatchCount << "\n";
    os << "Control Match Count: " << out.controlMatchCount << "\n";
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
