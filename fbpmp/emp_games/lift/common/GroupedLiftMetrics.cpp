/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "GroupedLiftMetrics.h"

#include <vector>

#include "folly/json.h"

#include <fbpcf/common/FunctionalUtil.h>
#include <fbpcf/common/VectorUtil.h>

namespace private_lift {
bool GroupedLiftMetrics::operator==(
    const GroupedLiftMetrics& other) const noexcept {
  return metrics == other.metrics && cohortMetrics == other.cohortMetrics;
}

GroupedLiftMetrics GroupedLiftMetrics::operator+(
    const GroupedLiftMetrics& other) const noexcept {
  return GroupedLiftMetrics{
      metrics + other.metrics,
      fbpcf::vector::Add(cohortMetrics, other.cohortMetrics)};
}

GroupedLiftMetrics GroupedLiftMetrics::operator^(
    const GroupedLiftMetrics& other) const noexcept {
  return GroupedLiftMetrics{
      metrics ^ other.metrics,
      fbpcf::vector::Xor(cohortMetrics, other.cohortMetrics)};
}

std::ostream& operator<<(
    std::ostream& os,
    const GroupedLiftMetrics& obj) noexcept {
  return os << obj.toJson();
}

std::string GroupedLiftMetrics::toJson() const {
  auto container = folly::dynamic::array();
  std::transform(
      cohortMetrics.begin(),
      cohortMetrics.end(),
      std::back_inserter(container),
      [](auto m) { return m.toDynamic(); });
  folly::dynamic obj = folly::dynamic::object("metrics", metrics.toDynamic())(
      "cohortMetrics", container);

  return folly::toJson(obj);
}

GroupedLiftMetrics GroupedLiftMetrics::fromJson(const std::string& str) {
  auto obj = folly::parseJson(str);
  std::vector<LiftMetrics> container;
  std::transform(
      obj["cohortMetrics"].begin(),
      obj["cohortMetrics"].end(),
      std::back_inserter(container),
      [](auto m) { return LiftMetrics::fromDynamic(m); });

  return GroupedLiftMetrics{
      LiftMetrics::fromDynamic(obj["metrics"]), container};
}
} // namespace private_lift
