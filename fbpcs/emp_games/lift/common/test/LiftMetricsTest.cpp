/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <gtest/gtest.h>

#include "folly/Random.h"

#include "../LiftMetrics.h"

namespace private_lift {
class LiftMetricsTest : public ::testing::Test {
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
    metrics_ = fakeLiftMetrics();
  }

  LiftMetrics metrics_;
};

TEST_F(LiftMetricsTest, LiftMetrics) {
  auto json = metrics_.toJson();
  auto parsedMetrics = LiftMetrics::fromJson(json);
  EXPECT_EQ(metrics_, parsedMetrics);
}

TEST_F(LiftMetricsTest, TestPlus) {
  LiftMetrics a{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, {0, 1}, {2, 3}};
  LiftMetrics b{
      15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, {4, 5}, {6, 7}};
  LiftMetrics expected{
      16, 18, 20, 22, 24, 26, 28, 30, 32, 34, 36, 38, 40, 42, {4, 6}, {8, 10}};
  EXPECT_EQ(expected, a + b);
}

TEST_F(LiftMetricsTest, TestXor) {
  LiftMetrics a{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, {0, 1}, {2, 3}};
  LiftMetrics b{
      15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, {4, 5}, {6, 7}};
  LiftMetrics expected{
      14, 18, 18, 22, 22, 18, 18, 30, 30, 18, 18, 22, 22, 18, {4, 4}, {4, 4}};
  EXPECT_EQ(expected, a ^ b);
}
} // namespace private_lift
