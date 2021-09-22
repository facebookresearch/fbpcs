/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <string>
#include <vector>

#include "folly/dynamic.h"

namespace private_lift {
/*
 * Simple struct representing the metrics in a Lift computation
 */
struct LiftMetrics {
  int64_t testPopulation;
  int64_t controlPopulation;
  int64_t testConversions;
  int64_t controlConversions;
  int64_t testConverters;
  int64_t controlConverters;
  int64_t testValue;
  int64_t controlValue;
  int64_t testValueSquared;
  int64_t controlValueSquared;
  int64_t testNumConvSquared;
  int64_t controlNumConvSquared;
  int64_t testMatchCount;
  int64_t controlMatchCount;
  int64_t testImpressions;
  int64_t controlImpressions;
  int64_t testClicks;
  int64_t controlClicks;
  int64_t testSpend;
  int64_t controlSpend;
  int64_t testReach;
  int64_t controlReach;
  int64_t testClickers;
  int64_t controlClickers;
  int64_t reachedConversions;
  int64_t reachedValue;
  std::vector<int64_t> testConvHistogram;
  std::vector<int64_t> controlConvHistogram;

  bool operator==(const LiftMetrics& other) const noexcept;
  LiftMetrics operator+(const LiftMetrics& other) const noexcept;
  LiftMetrics operator^(const LiftMetrics& other) const noexcept;
  // required for gtest to output failing tests in a human-readable format
  friend std::ostream& operator<<(
      std::ostream& os,
      const LiftMetrics& obj) noexcept;

  std::string toJson() const;
  static LiftMetrics fromJson(const std::string& str);

 private:
  folly::dynamic toDynamic() const;
  static LiftMetrics fromDynamic(const folly::dynamic& obj);

  friend struct GroupedLiftMetrics;
};
} // namespace private_lift
