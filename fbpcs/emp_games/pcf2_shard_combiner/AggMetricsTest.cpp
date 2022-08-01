/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <gtest/gtest.h>

#include <fbpcf/exception/exceptions.h>
#include <fbpcf/io/api/FileIOWrappers.h>
#include <fbpcs/emp_games/common/Constants.h>
#include <fbpcs/emp_games/common/TestUtil.h>
#include <fbpcs/emp_games/pcf2_shard_combiner/AggMetrics.h>
#include <fbpcs/emp_games/pcf2_shard_combiner/AggMetrics_impl.h>
using namespace ::testing;

namespace shard_combiner {

class AggMetricsTest : public ::testing::Test {
 protected:
  void SetUp() override {
    std::string filePath = __FILE__;
    baseDir_ = filePath.substr(0, filePath.rfind("/")) + "/test/";
  }
  std::string baseDir_;
};

TEST_F(AggMetricsTest, TestParseAttribution) {
  auto inputPath =
      baseDir_ + "shard_validation_test/valid_measurement_shard.json";

  auto parsedInput =
      folly::parseJson(fbpcf::io::FileIOWrappers::readFile(inputPath));

  constexpr int schedulerId = 0;
  constexpr bool usingBatch = false;
  constexpr common::InputEncryption inputEncryption =
      common::InputEncryption::Plaintext;

  auto metrics =
      AggMetrics<schedulerId, usingBatch, inputEncryption>::fromJson(inputPath);

  XLOG(INFO) << metrics;
  EXPECT_EQ(
      metrics->getAtKey("last_click_1d")
          ->getAtKey("measurement")
          ->getAtKey("1")
          ->getAtKey("convs")
          ->getValue(),
      -831273128088263600);
  EXPECT_EQ(
      metrics->getAtKey("last_click_1d")
          ->getAtKey("measurement")
          ->getAtKey("1")
          ->getAtKey("sales")
          ->getValue(),
      339959610281870460);
  EXPECT_EQ(
      metrics->getAtKey("last_touch_1d")
          ->getAtKey("measurement")
          ->getAtKey("1")
          ->getAtKey("convs")
          ->getValue(),
      -4250297646419635700);
  EXPECT_EQ(
      metrics->getAtKey("last_touch_1d")
          ->getAtKey("measurement")
          ->getAtKey("1")
          ->getAtKey("sales")
          ->getValue(),
      -572762462605311500);
  EXPECT_EQ(metrics->toDynamic(), parsedInput);
}

TEST_F(AggMetricsTest, TestParseLift) {
  auto inputPath = baseDir_ + "shard_validation_test/valid_lift_input.json";
  auto parsedInput =
      folly::parseJson(fbpcf::io::FileIOWrappers::readFile(inputPath));

  constexpr int schedulerId = 0;
  constexpr bool usingBatch = false;
  constexpr common::InputEncryption inputEncryption =
      common::InputEncryption::Plaintext;

  auto metrics =
      AggMetrics<schedulerId, usingBatch, inputEncryption>::fromJson(inputPath);
  XLOG(INFO) << metrics;

  ASSERT_EQ(metrics->getAtKey("cohortMetrics")->getAsList().size(), 2);
  ASSERT_EQ(metrics->getAtKey("publisherBreakdowns")->getAsList().size(), 2);
  ASSERT_EQ(metrics->getAtKey("metrics")->getAsDict().size(), 28);

  // check a few values
  EXPECT_EQ(
      metrics->getAtKey("cohortMetrics")
          ->getAtIndex(0)
          ->getAtKey("controlValueSquared")
          ->getValue(),
      2988483738);
  EXPECT_EQ(
      metrics->getAtKey("cohortMetrics")
          ->getAtIndex(0)
          ->getAtKey("reachedValue")
          ->getValue(),
      1957171223);
  EXPECT_EQ(
      metrics->getAtKey("cohortMetrics")
          ->getAtIndex(1)
          ->getAtKey("controlValueSquared")
          ->getValue(),
      1825398531);
  EXPECT_EQ(
      metrics->getAtKey("cohortMetrics")
          ->getAtIndex(1)
          ->getAtKey("reachedValue")
          ->getValue(),
      2368649346);
  EXPECT_EQ(
      metrics->getAtKey("publisherBreakdowns")
          ->getAtIndex(0)
          ->getAtKey("controlValueSquared")
          ->getValue(),
      2988483738);
  EXPECT_EQ(
      metrics->getAtKey("publisherBreakdowns")
          ->getAtIndex(0)
          ->getAtKey("reachedValue")
          ->getValue(),
      1957171223);
  EXPECT_EQ(
      metrics->getAtKey("publisherBreakdowns")
          ->getAtIndex(1)
          ->getAtKey("controlValueSquared")
          ->getValue(),
      1825398531);
  EXPECT_EQ(
      metrics->getAtKey("publisherBreakdowns")
          ->getAtIndex(1)
          ->getAtKey("reachedValue")
          ->getValue(),
      2368649346);
  EXPECT_EQ(
      metrics->getAtKey("metrics")->getAtKey("controlValueSquared")->getValue(),
      405497006);
  EXPECT_EQ(metrics->toDynamic(), parsedInput);
}

TEST_F(AggMetricsTest, TestParseInvalidMap) {
  auto inputPath = baseDir_ + "test_new_parser/invalid_map.json";

  EXPECT_THROW(
      AggMetrics<>::fromJson(inputPath),
      common::exceptions::NotImplementedError);
}

TEST_F(AggMetricsTest, AccumulatePlainTextTest) {
  auto inputPath1 =
      baseDir_ + "test_new_parser/accumulate_test_input_plaintext_1.json";
  auto inputPath2 =
      baseDir_ + "test_new_parser/accumulate_test_input_plaintext_2.json";
  auto expectedResultPath =
      baseDir_ + "test_new_parser/accumulate_test_result_plaintext.json";

  auto input1 = AggMetrics<>::fromJson(inputPath1);
  auto input2 = AggMetrics<>::fromJson(inputPath2);
  auto expectedResultDynObj =
      folly::parseJson(fbpcf::io::FileIOWrappers::readFile(expectedResultPath));

  auto result = AggMetrics<>::newLike(input1);

  AggMetrics<>::accumulate(result, input1);
  AggMetrics<>::accumulate(result, input2);

  EXPECT_EQ(result->toDynamic(), expectedResultDynObj);
}

} // namespace shard_combiner
