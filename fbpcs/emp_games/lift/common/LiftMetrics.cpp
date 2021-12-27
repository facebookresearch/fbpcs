/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "LiftMetrics.h"

#include <cstdint>
#include <vector>

#include "folly/dynamic.h"
#include "folly/json.h"

#include <fbpcf/common/FunctionalUtil.h>
#include <fbpcf/common/VectorUtil.h>

namespace private_lift {
bool LiftMetrics::operator==(const LiftMetrics& other) const noexcept {
  return testConversions == other.testConversions &&
      controlConversions == other.controlConversions &&
      testConverters == other.testConverters &&
      controlConverters == other.controlConverters &&
      testValue == other.testValue && controlValue == other.controlValue &&
      testValueSquared == other.testValueSquared &&
      controlValueSquared == other.controlValueSquared &&
      testNumConvSquared == other.testNumConvSquared &&
      controlNumConvSquared == other.controlNumConvSquared &&
      testMatchCount == other.testMatchCount &&
      controlMatchCount == other.controlMatchCount &&
      reachedConversions == other.reachedConversions &&
      reachedValue == other.reachedValue &&
      testConvHistogram == other.testConvHistogram &&
      controlConvHistogram == other.controlConvHistogram;
}

LiftMetrics LiftMetrics::operator+(const LiftMetrics& other) const noexcept {
  std::vector<int64_t> addedTestConvHistogram;
  // TODO: Could be replaced with zip_longest and map
  // Iterate over the longer histogram
  for (size_t i = 0;
       i < testConvHistogram.size() || i < other.testConvHistogram.size();
       ++i) {
    auto a = i < testConvHistogram.size() ? testConvHistogram.at(i) : 0;
    auto b =
        i < other.testConvHistogram.size() ? other.testConvHistogram.at(i) : 0;
    addedTestConvHistogram.push_back(a + b);
  }

  std::vector<int64_t> addedControlConvHistogram;
  for (size_t i = 0;
       i < controlConvHistogram.size() || i < other.controlConvHistogram.size();
       ++i) {
    auto a = i < controlConvHistogram.size() ? controlConvHistogram.at(i) : 0;
    auto b = i < other.controlConvHistogram.size()
        ? other.controlConvHistogram.at(i)
        : 0;
    addedControlConvHistogram.push_back(a + b);
  }

  return LiftMetrics{
      testConversions + other.testConversions,
      controlConversions + other.controlConversions,
      testConverters + other.testConverters,
      controlConverters + other.controlConverters,
      testValue + other.testValue,
      controlValue + other.controlValue,
      testValueSquared + other.testValueSquared,
      controlValueSquared + other.controlValueSquared,
      testNumConvSquared + other.testNumConvSquared,
      controlNumConvSquared + other.controlNumConvSquared,
      testMatchCount + other.testMatchCount,
      controlMatchCount + other.controlMatchCount,
      reachedConversions + other.reachedConversions,
      reachedValue + other.reachedValue,
      addedTestConvHistogram,
      addedControlConvHistogram};
}

LiftMetrics LiftMetrics::operator^(const LiftMetrics& other) const noexcept {
  std::vector<int64_t> xoredTestConvHistogram;
  // TODO: Could be replaced with zip_longest and map
  // Iterate over the longer histogram
  for (size_t i = 0;
       i < testConvHistogram.size() || i < other.testConvHistogram.size();
       ++i) {
    auto a = i < testConvHistogram.size() ? testConvHistogram.at(i) : 0;
    auto b =
        i < other.testConvHistogram.size() ? other.testConvHistogram.at(i) : 0;
    xoredTestConvHistogram.push_back(a ^ b);
  }

  std::vector<int64_t> xoredControlConvHistogram;
  // Iterate over the longer histogram
  for (size_t i = 0;
       i < controlConvHistogram.size() || i < other.controlConvHistogram.size();
       ++i) {
    auto a = i < controlConvHistogram.size() ? controlConvHistogram.at(i) : 0;
    auto b = i < other.controlConvHistogram.size()
        ? other.controlConvHistogram.at(i)
        : 0;
    xoredControlConvHistogram.push_back(a ^ b);
  }

  return LiftMetrics{
      testConversions ^ other.testConversions,
      controlConversions ^ other.controlConversions,
      testConverters ^ other.testConverters,
      controlConverters ^ other.controlConverters,
      testValue ^ other.testValue,
      controlValue ^ other.controlValue,
      testValueSquared ^ other.testValueSquared,
      controlValueSquared ^ other.controlValueSquared,
      testNumConvSquared ^ other.testNumConvSquared,
      controlNumConvSquared ^ other.controlNumConvSquared,
      testMatchCount ^ other.testMatchCount,
      controlMatchCount ^ other.controlMatchCount,
      reachedConversions ^ other.reachedConversions,
      reachedValue ^ other.reachedValue,
      xoredTestConvHistogram,
      xoredControlConvHistogram};
}

std::ostream& operator<<(std::ostream& os, const LiftMetrics& obj) noexcept {
  return os << obj.toJson();
}

std::string LiftMetrics::toJson() const {
  auto obj = toDynamic();
  return folly::toJson(obj);
}

LiftMetrics LiftMetrics::fromJson(const std::string& str) {
  auto obj = folly::parseJson(str);
  return fromDynamic(obj);
}

folly::dynamic LiftMetrics::toDynamic() const {
  auto testConvHistogramDynamic =
      folly::dynamic(testConvHistogram.begin(), testConvHistogram.end());
  auto controlConvHistogramDynamic =
      folly::dynamic(controlConvHistogram.begin(), controlConvHistogram.end());

  return folly::dynamic::object("testConversions", testConversions)(
      "controlConversions", controlConversions)(
      "testConverters", testConverters)("controlConverters", controlConverters)("testValue", testValue)("controlValue", controlValue)("testValueSquared", testValueSquared)("controlValueSquared", controlValueSquared)("testNumConvSquared", testNumConvSquared)("controlNumConvSquared", controlNumConvSquared)("testMatchCount", testMatchCount)("controlMatchCount", controlMatchCount)("reachedConversions", reachedConversions)("reachedValue", reachedValue)("testConvHistogram", testConvHistogramDynamic)("controlConvHistogram", controlConvHistogramDynamic);
}

LiftMetrics LiftMetrics::fromDynamic(const folly::dynamic& obj) {
  LiftMetrics metrics;
  std::vector<int64_t> testConvHistogram;
  std::vector<int64_t> controlConvHistogram;

  metrics.testConversions = obj["testConversions"].asInt();
  metrics.controlConversions = obj["controlConversions"].asInt();
  metrics.testConverters = obj["testConverters"].asInt();
  metrics.controlConverters = obj["controlConverters"].asInt();
  metrics.testValue = obj["testValue"].asInt();
  metrics.controlValue = obj["controlValue"].asInt();
  metrics.testValueSquared = obj["testValueSquared"].asInt();
  metrics.controlValueSquared = obj["controlValueSquared"].asInt();
  metrics.testNumConvSquared = obj["testNumConvSquared"].asInt();
  metrics.controlNumConvSquared = obj["controlNumConvSquared"].asInt();
  metrics.testMatchCount = obj["testMatchCount"].asInt();
  metrics.controlMatchCount = obj["controlMatchCount"].asInt();
  metrics.reachedConversions = obj["reachedConversions"].asInt();
  metrics.reachedValue = obj["reachedValue"].asInt();

  for (const auto& val : obj["testConvHistogram"]) {
    testConvHistogram.push_back(val.asInt());
  }
  metrics.testConvHistogram = std::move(testConvHistogram);

  for (const auto& val : obj["controlConvHistogram"]) {
    controlConvHistogram.push_back(val.asInt());
  }
  metrics.controlConvHistogram = std::move(controlConvHistogram);

  return metrics;
}
} // namespace private_lift
