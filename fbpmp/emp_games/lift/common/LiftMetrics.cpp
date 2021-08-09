/*
 * Copyright (c) Facebook, Inc. and its affiliates.
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
  return testPopulation == other.testPopulation &&
      controlPopulation == other.controlPopulation &&
      testConversions == other.testConversions &&
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
      testImpressions == other.testImpressions &&
      controlImpressions == other.controlImpressions &&
      testClicks == other.testClicks && controlClicks == other.controlClicks &&
      testSpend == other.testSpend && controlSpend == other.controlSpend &&
      testReach == other.testReach && controlReach == other.controlReach &&
      testClickers == other.testClickers &&
      controlClickers == other.controlClickers &&
      reachedConversions == other.reachedConversions &&
      reachedValue == other.reachedValue;
}

LiftMetrics LiftMetrics::operator+(const LiftMetrics& other) const noexcept {
  return LiftMetrics{
      testPopulation + other.testPopulation,
      controlPopulation + other.controlPopulation,
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
      testImpressions + other.testImpressions,
      controlImpressions + other.controlImpressions,
      testClicks + other.testClicks,
      controlClicks + other.controlClicks,
      testSpend + other.testSpend,
      controlSpend + other.controlSpend,
      testReach + other.testReach,
      controlReach + other.controlReach,
      testClickers + other.testClickers,
      controlClickers + other.controlClickers,
      reachedConversions + other.reachedConversions,
      reachedValue + other.reachedValue};
}

LiftMetrics LiftMetrics::operator^(const LiftMetrics& other) const noexcept {
  return LiftMetrics{
      testPopulation ^ other.testPopulation,
      controlPopulation ^ other.controlPopulation,
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
      testImpressions ^ other.testImpressions,
      controlImpressions ^ other.controlImpressions,
      testClicks ^ other.testClicks,
      controlClicks ^ other.controlClicks,
      testSpend ^ other.testSpend,
      controlSpend ^ other.controlSpend,
      testReach ^ other.testReach,
      controlReach ^ other.controlReach,
      testClickers ^ other.testClickers,
      controlClickers ^ other.controlClickers,
      reachedConversions ^ other.reachedConversions,
      reachedValue ^ other.reachedValue};
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
  return folly::dynamic::object(
      "testPopulation", testPopulation)(
      "controlPopulation", controlPopulation)(
      "testConversions", testConversions)(
      "controlConversions", controlConversions)(
      "testConverters", testConverters)(
      "controlConverters", controlConverters)(
      "testValue", testValue)(
      "controlValue", controlValue)(
      "testValueSquared", testValueSquared)(
      "controlValueSquared", controlValueSquared)(
      "testNumConvSquared", testNumConvSquared)(
      "controlNumConvSquared", controlNumConvSquared)(
      "testMatchCount", testMatchCount)(
      "controlMatchCount", controlMatchCount)(
      "testImpressions", testImpressions)(
      "controlImpressions", controlImpressions)(
      "testClicks", testClicks)(
      "controlClicks", controlClicks)(
      "testSpend", testSpend)(
      "controlSpend", controlSpend)(
      "testReach", testReach)(
      "controlReach", controlReach)(
      "testClickers", testClickers)(
      "controlClickers", controlClickers)(
      "reachedConversions", reachedConversions)(
      "reachedValue", reachedValue);
}

LiftMetrics LiftMetrics::fromDynamic(const folly::dynamic& obj) {
  LiftMetrics metrics;

  metrics.testPopulation = obj["testPopulation"].asInt();
  metrics.controlPopulation = obj["controlPopulation"].asInt();
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
  metrics.testImpressions = obj["testImpressions"].asInt();
  metrics.controlImpressions = obj["controlImpressions"].asInt();
  metrics.testClicks = obj["testClicks"].asInt();
  metrics.controlClicks = obj["controlClicks"].asInt();
  metrics.testSpend = obj["testSpend"].asInt();
  metrics.controlSpend = obj["controlSpend"].asInt();
  metrics.testReach = obj["testReach"].asInt();
  metrics.controlReach = obj["controlReach"].asInt();
  metrics.testClickers = obj["testClickers"].asInt();
  metrics.controlClickers = obj["controlClickers"].asInt();
  metrics.reachedConversions = obj["reachedConversions"].asInt();
  metrics.reachedValue = obj["reachedValue"].asInt();

  return metrics;
}
} // namespace private_lift
