/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <gtest/gtest.h>

#include "folly/Random.h"

#include "../GroupedLiftMetrics.h"

namespace private_lift {
class GroupedLiftMetricsTest : public ::testing::Test {
 private:
  LiftMetrics fakeLiftMetrics() {
    auto r = []() { return folly::Random::rand32(); };
    return LiftMetrics{
        r(),
        r(),
        r(),
        r(),
        r(),
        r(),
        r(),
        r(),
        r(),
        r(),
        r(),
        r(),
        r(),
        r(),
        {r()},
        {r()}};
  }

 protected:
  void SetUp() override {
    groupedMetrics_ = GroupedLiftMetrics{
        fakeLiftMetrics(), {fakeLiftMetrics(), fakeLiftMetrics()}};
  }

  GroupedLiftMetrics groupedMetrics_;
};

TEST_F(GroupedLiftMetricsTest, GroupedLiftMetrics) {
  auto json = groupedMetrics_.toJson();
  auto parsedMetrics = GroupedLiftMetrics::fromJson(json);
  EXPECT_EQ(groupedMetrics_.metrics, parsedMetrics.metrics);
  EXPECT_EQ(groupedMetrics_.cohortMetrics, parsedMetrics.cohortMetrics);
}
} // namespace private_lift
