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

// More info available here:
// https://docs.google.com/document/d/1K6HgzmuBX1dOwADxCYutAtcqYZR9vq4lzB8EZ6qqR8E/edit#
inline constexpr uint64_t kNumDefaultCohorts = 4;
inline constexpr uint64_t kNumPublisherBreakdown = 2;

struct GroupedLiftMetrics {
  LiftMetrics metrics;
  std::vector<LiftMetrics> cohortMetrics;
  std::vector<LiftMetrics> publisherBreakdowns;

  GroupedLiftMetrics();
  GroupedLiftMetrics(uint64_t numCohorts, uint64_t numPublisheBreakdown);

  GroupedLiftMetrics(
      const LiftMetrics& metrics,
      const std::vector<LiftMetrics>& cohort,
      const std::vector<LiftMetrics>& publisherBreakDown)
      : metrics(metrics),
        cohortMetrics(cohort),
        publisherBreakdowns(publisherBreakDown) {}

  bool operator==(const GroupedLiftMetrics& other) const noexcept;
  GroupedLiftMetrics operator+(const GroupedLiftMetrics& other) const noexcept;
  GroupedLiftMetrics operator^(const GroupedLiftMetrics& other) const noexcept;
  // required for gtest to output failing tests in a human-readable format
  friend std::ostream& operator<<(
      std::ostream& os,
      const GroupedLiftMetrics& obj) noexcept;

  std::string toJson() const;
  static GroupedLiftMetrics fromJson(const std::string& str);

  void reset() {
    metrics.reset();
    for (auto& cohort : cohortMetrics) {
      cohort.reset();
    }
    for (auto& pb : publisherBreakdowns) {
      pb.reset();
    }
  }
};

} // namespace private_lift
