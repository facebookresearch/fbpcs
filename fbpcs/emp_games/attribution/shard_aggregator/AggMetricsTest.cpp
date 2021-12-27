/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "AggMetrics.h"

#include <map>
#include <vector>

#include <gtest/gtest-death-test.h>
#include <gtest/gtest.h>

#include <folly/dynamic.h>
#include <folly/json.h>
#include <folly/logging/xlog.h>
#include <folly/test/JsonTestUtil.h>

#include <fbpcf/io/FileManagerUtil.h>

namespace measurement::private_attribution {

class AggMetricsTest : public ::testing::Test {
 protected:
  void SetUp() override {
    // Get full path of current source file
    std::string filePath = __FILE__;
    baseDir_ = filePath.substr(0, filePath.rfind("/")) + "/test/";
  }

  std::string baseDir_;
};

TEST_F(AggMetricsTest, TestParseSimpleMap) {
  auto inputPath = baseDir_ + "test_new_parser/simple_map.json";
  auto parsedInput = folly::parseJson(fbpcf::io::read(inputPath));
  auto metrics = private_measurement::AggMetrics::fromDynamic(parsedInput);
  XLOG(INFO) << metrics;
  EXPECT_EQ(metrics.getAtKey("measurement")->getIntValue(), 339959610281870460);
  EXPECT_EQ(metrics.toDynamic(), parsedInput);
}

TEST_F(AggMetricsTest, TestParseAttribution) {
  auto inputPath =
      baseDir_ + "shard_validation_test/valid_measurement_shard.json";
  auto parsedInput = folly::parseJson(fbpcf::io::read(inputPath));
  auto metrics = private_measurement::AggMetrics::fromDynamic(parsedInput);
  XLOG(INFO) << metrics;
  EXPECT_EQ(
      metrics.getAtKey("last_click_1d")
          ->getAtKey("measurement")
          ->getAtKey("1")
          ->getAtKey("convs")
          ->getIntValue(),
      -831273128088263600);
  EXPECT_EQ(
      metrics.getAtKey("last_click_1d")
          ->getAtKey("measurement")
          ->getAtKey("1")
          ->getAtKey("sales")
          ->getIntValue(),
      339959610281870460);
  EXPECT_EQ(
      metrics.getAtKey("last_touch_1d")
          ->getAtKey("measurement")
          ->getAtKey("1")
          ->getAtKey("convs")
          ->getIntValue(),
      -4250297646419635700);
  EXPECT_EQ(
      metrics.getAtKey("last_touch_1d")
          ->getAtKey("measurement")
          ->getAtKey("1")
          ->getAtKey("sales")
          ->getIntValue(),
      -572762462605311500);
  EXPECT_EQ(metrics.toDynamic(), parsedInput);
}

TEST_F(AggMetricsTest, TestParseLift) {
  auto inputPath = baseDir_ + "shard_validation_test/valid_lift_input.json";
  auto parsedInput = folly::parseJson(fbpcf::io::read(inputPath));
  auto metrics = private_measurement::AggMetrics::fromDynamic(parsedInput);
  XLOG(INFO) << metrics;

  ASSERT_EQ(metrics.getAtKey("cohortMetrics")->getAsList().size(), 2);
  ASSERT_EQ(metrics.getAtKey("publisherBreakdowns")->getAsList().size(), 2);
  ASSERT_EQ(metrics.getAtKey("metrics")->getAsMap().size(), 28);

  // check a few values
  EXPECT_EQ(
      metrics.getAtKey("cohortMetrics")
          ->getAtIndex(0)
          ->getAtKey("controlValueSquared")
          ->getIntValue(),
      2988483738);
  EXPECT_EQ(
      metrics.getAtKey("cohortMetrics")
          ->getAtIndex(0)
          ->getAtKey("reachedValue")
          ->getIntValue(),
      1957171223);
  EXPECT_EQ(
      metrics.getAtKey("cohortMetrics")
          ->getAtIndex(1)
          ->getAtKey("controlValueSquared")
          ->getIntValue(),
      1825398531);
  EXPECT_EQ(
      metrics.getAtKey("cohortMetrics")
          ->getAtIndex(1)
          ->getAtKey("reachedValue")
          ->getIntValue(),
      2368649346);
  EXPECT_EQ(
      metrics.getAtKey("publisherBreakdowns")
          ->getAtIndex(0)
          ->getAtKey("controlValueSquared")
          ->getIntValue(),
      2988483738);
  EXPECT_EQ(
      metrics.getAtKey("publisherBreakdowns")
          ->getAtIndex(0)
          ->getAtKey("reachedValue")
          ->getIntValue(),
      1957171223);
  EXPECT_EQ(
      metrics.getAtKey("publisherBreakdowns")
          ->getAtIndex(1)
          ->getAtKey("controlValueSquared")
          ->getIntValue(),
      1825398531);
  EXPECT_EQ(
      metrics.getAtKey("publisherBreakdowns")
          ->getAtIndex(1)
          ->getAtKey("reachedValue")
          ->getIntValue(),
      2368649346);
  EXPECT_EQ(
      metrics.getAtKey("metrics")
          ->getAtKey("controlValueSquared")
          ->getIntValue(),
      405497006);
  EXPECT_EQ(metrics.toDynamic(), parsedInput);
}

TEST_F(AggMetricsTest, TestParseInvalidMap) {
  auto inputPath = baseDir_ + "test_new_parser/invalid_map.json";
  EXPECT_DEATH(
      private_measurement::AggMetrics::fromDynamic(
          folly::parseJson(fbpcf::io::read(inputPath))),
      "Metric values should be integers");
}
} // namespace measurement::private_attribution
