/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <gtest/gtest.h>

#include <fbpcf/exception/exceptions.h>
#include <fbpcs/emp_games/pcf2_shard_combiner/AggMetrics.h>
#include <fbpcs/emp_games/pcf2_shard_combiner/ShardValidator.h>
#include <fbpcs/emp_games/pcf2_shard_combiner/ShardValidator_impl.h>

namespace shard_combiner {

class ShardValidatorTest : public ::testing::Test {
 protected:
  void SetUp() override {
    // Get full path of current source file
    std::string filePath = __FILE__;
    baseDir_ = filePath.substr(0, filePath.rfind("/")) +
        "/test/shard_validation_test/";
  }
  std::string baseDir_;
  static constexpr int schedulerId = 0;
  static constexpr bool usingBatch = false;
  static constexpr common::InputEncryption inputEncryption =
      common::InputEncryption::Plaintext;
};

// Tests for ad object format validation
TEST_F(ShardValidatorTest, AdObjectTestValidMeasurementInput) {
  std::string inputFile = baseDir_ + "valid_measurement_shard.json";

  auto testMetricsObj =
      AggMetrics<schedulerId, usingBatch, inputEncryption>::fromJson(inputFile);

  validateShardSchema<ShardSchemaType::kAdObjFormat>(*testMetricsObj);
}

// Tests for GroupedLiftMetrics format validation
TEST_F(ShardValidatorTest, GroupedLiftMetricsTest) {
  std::string inputFile = baseDir_ + "valid_lift_input.json";

  auto testMetricsObj =
      AggMetrics<schedulerId, usingBatch, inputEncryption>::fromJson(inputFile);

  validateShardSchema<ShardSchemaType::kGroupedLiftMetrics>(*testMetricsObj);
}

// Tests if the validator emits exeception for wrong formats.
TEST_F(ShardValidatorTest, AdObjectTestValidIncorrectMeasurementInput) {
  std::string inputFile = baseDir_ + "valid_lift_input.json";

  auto testMetricsObj =
      AggMetrics<schedulerId, usingBatch, inputEncryption>::fromJson(inputFile);

  EXPECT_THROW(
      validateShardSchema<ShardSchemaType::kAdObjFormat>(*testMetricsObj),
      common::exceptions::SchemaTraceError);
}

// Tests if the validator emits exeception for wrong formats.
TEST_F(ShardValidatorTest, GroupedLiftMetricsIncorrectInput) {
  std::string inputFile = baseDir_ + "valid_measurement_shard.json";

  auto testMetricsObj =
      AggMetrics<schedulerId, usingBatch, inputEncryption>::fromJson(inputFile);

  EXPECT_THROW(
      validateShardSchema<ShardSchemaType::kGroupedLiftMetrics>(
          *testMetricsObj),
      common::exceptions::SchemaTraceError);
}

TEST_F(ShardValidatorTest, AdObjectTestInvalidAggregationName) {
  std::string inputFile = baseDir_ + "invalid_aggregation_name.json";
  auto testMetricsObj =
      AggMetrics<schedulerId, usingBatch, inputEncryption>::fromJson(inputFile);
  EXPECT_THROW(
      validateShardSchema<ShardSchemaType::kAdObjFormat>(*testMetricsObj),
      common::exceptions::SchemaTraceError);
}

// Tests for lift format validation
TEST_F(ShardValidatorTest, LiftTestValidLiftInput) {
  std::string inputFile = baseDir_ + "valid_lift_input.json";
  auto testMetricsObj =
      AggMetrics<schedulerId, usingBatch, inputEncryption>::fromJson(inputFile);
  validateShardSchema<ShardSchemaType::kGroupedLiftMetrics>(*testMetricsObj);
}

TEST_F(ShardValidatorTest, LiftTestInvalidAdObjectInput) {
  std::string inputFile = baseDir_ + "valid_measurement_shard.json";
  auto testMetricsObj =
      AggMetrics<schedulerId, usingBatch, inputEncryption>::fromJson(inputFile);
  EXPECT_THROW(
      validateShardSchema<ShardSchemaType::kGroupedLiftMetrics>(
          *testMetricsObj),
      common::exceptions::SchemaTraceError);
}

TEST_F(ShardValidatorTest, LiftTestInvalidInputEmptyMap) {
  std::string inputFile = baseDir_ + "invalid_empty_map_0.json";
  auto testMetricsObj =
      AggMetrics<schedulerId, usingBatch, inputEncryption>::fromJson(inputFile);
  EXPECT_THROW(
      validateShardSchema<ShardSchemaType::kGroupedLiftMetrics>(
          *testMetricsObj),
      common::exceptions::SchemaTraceError);
}

TEST_F(ShardValidatorTest, LiftTestValidInputEmptyCohortMetrics) {
  std::string inputFile = baseDir_ + "valid_lift_no_cohort_metrics.json";
  auto testMetricsObj =
      AggMetrics<schedulerId, usingBatch, inputEncryption>::fromJson(inputFile);

  validateShardSchema<ShardSchemaType::kGroupedLiftMetrics>(*testMetricsObj);
}

} // namespace shard_combiner
