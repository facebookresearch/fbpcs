/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
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
  int64_t reachedConversions;
  int64_t reachedValue;
  std::vector<int64_t> testConvHistogram;
  std::vector<int64_t> controlConvHistogram;

  LiftMetrics() {}

  LiftMetrics(
      int64_t testConversions_,
      int64_t controlConversions_,
      int64_t testConverters_,
      int64_t controlConverters_,
      int64_t testValue_,
      int64_t controlValue_,
      int64_t testValueSquared_,
      int64_t controlValueSquared_,
      int64_t testNumConvSquared_,
      int64_t controlNumConvSquared_,
      int64_t testMatchCount_,
      int64_t controlMatchCount_,
      int64_t reachedConversions_,
      int64_t reachedValue_,
      std::vector<int64_t> testConvHistogram_,
      std::vector<int64_t> controlConvHistogram_)
      : testConversions{testConversions_},
        controlConversions{controlConversions_},
        testConverters{testConverters_},
        controlConverters{controlConverters_},
        testValue{testValue_},
        controlValue{controlValue_},
        testValueSquared{testValueSquared_},
        controlValueSquared{controlValueSquared_},
        testNumConvSquared{testNumConvSquared_},
        controlNumConvSquared{controlNumConvSquared_},
        testMatchCount{testMatchCount_},
        controlMatchCount{controlMatchCount_},
        reachedConversions{reachedConversions_},
        reachedValue{reachedValue_},
        testConvHistogram{std::move(testConvHistogram_)},
        controlConvHistogram{std::move(controlConvHistogram_)} {}

  bool operator==(const LiftMetrics& other) const noexcept;
  LiftMetrics operator+(const LiftMetrics& other) const noexcept;
  LiftMetrics operator^(const LiftMetrics& other) const noexcept;
  // required for gtest to output failing tests in a human-readable format
  friend std::ostream& operator<<(
      std::ostream& os,
      const LiftMetrics& obj) noexcept;

  std::string toJson() const;
  static LiftMetrics fromJson(const std::string& str);

  void reset() {
    this->testConversions = 0;
    this->controlConversions = 0;
    this->testConverters = 0;
    this->controlConverters = 0;
    this->testValue = 0;
    this->controlValue = 0;
    this->testValueSquared = 0;
    this->controlValueSquared = 0;
    this->testNumConvSquared = 0;
    this->controlNumConvSquared = 0;
    this->testMatchCount = 0;
    this->controlMatchCount = 0;
    this->reachedConversions = 0;
    this->reachedValue = 0;
    this->testConvHistogram.clear();
    this->controlConvHistogram.clear();
  }

 private:
  folly::dynamic toDynamic() const;
  static LiftMetrics fromDynamic(const folly::dynamic& obj);

  friend struct GroupedLiftMetrics;
};
} // namespace private_lift
