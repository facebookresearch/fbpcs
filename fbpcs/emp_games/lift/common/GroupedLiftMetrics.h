/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <string>
#include <vector>

#include "LiftMetrics.h"

namespace private_lift {
struct GroupedLiftMetrics {
  LiftMetrics metrics;
  std::vector<LiftMetrics> cohortMetrics;
  std::vector<LiftMetrics> publisherBreakdowns;

  bool operator==(const GroupedLiftMetrics& other) const noexcept;
  GroupedLiftMetrics operator+(const GroupedLiftMetrics& other) const noexcept;
  GroupedLiftMetrics operator^(const GroupedLiftMetrics& other) const noexcept;
  // required for gtest to output failing tests in a human-readable format
  friend std::ostream& operator<<(
      std::ostream& os,
      const GroupedLiftMetrics& obj) noexcept;

  std::string toJson() const;
  static GroupedLiftMetrics fromJson(const std::string& str);
};

} // namespace private_lift
