/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <map>
#include <vector>

#include <folly/dynamic.h>
#include <folly/json.h>
#include "folly/logging/xlog.h"

#include <fbpcf/common/FunctionalUtil.h>
#include <fbpcf/io/FileManagerUtil.h>

#include <gtest/gtest.h>

#include "ShardAggregatorValidation.h"

namespace measurement::private_attribution {

class ShardAggregatorValidationTest : public ::testing::Test {
 protected:
  void SetUp() override {
    // Get full path of current source file
    std::string filePath = __FILE__;
    baseDir_ = filePath.substr(0, filePath.rfind("/")) +
        "/test/shard_validation_test/";
  }

  std::string baseDir_;
};

// Tests for original validation function
TEST_F(ShardAggregatorValidationTest, TestValidMeasurementInput) {
  auto validMap = folly::parseJson(
      fbpcf::io::read(baseDir_ + "valid_measurement_shard.json"));
  auto validData = std::vector<folly::dynamic>({validMap});
  validateInputData(validData);
}

TEST_F(ShardAggregatorValidationTest, TestInvalidInputPCM) {
  auto invalidMap =
      folly::parseJson(fbpcf::io::read(baseDir_ + "invalid_pcm_shard.json"));
  auto invalidData = std::vector<folly::dynamic>({invalidMap});
  EXPECT_THROW(validateInputData(invalidData), InvalidFormatException);
}

TEST_F(ShardAggregatorValidationTest, TestInvalidInputLift) {
  // should throw an error for lift inputs -- testing with aggregator_alice_0
  auto invalidMap =
      folly::parseJson(fbpcf::io::read(baseDir_ + "valid_lift_input.json"));
  auto invalidData = std::vector<folly::dynamic>({invalidMap});
  EXPECT_THROW(validateInputData(invalidData), InvalidFormatException);
}

TEST_F(ShardAggregatorValidationTest, TestInvalidInputBadStructure) {
  auto invalidMap = folly::parseJson(
      fbpcf::io::read(baseDir_ + "invalid_bad_structure.json"));
  auto invalidData = std::vector<folly::dynamic>({invalidMap});
  EXPECT_THROW(validateInputData(invalidData), InvalidFormatException);
}

TEST_F(ShardAggregatorValidationTest, TestInvalidInputEmptyMap0) {
  auto invalidMap =
      folly::parseJson(fbpcf::io::read(baseDir_ + "invalid_empty_map_0.json"));
  auto invalidData = std::vector<folly::dynamic>({invalidMap});
  EXPECT_THROW(validateInputData(invalidData), InvalidFormatException);
}

TEST_F(ShardAggregatorValidationTest, TestInvalidInputEmptyMap1) {
  auto invalidMap =
      folly::parseJson(fbpcf::io::read(baseDir_ + "invalid_empty_map_1.json"));
  auto invalidData = std::vector<folly::dynamic>({invalidMap});
  EXPECT_THROW(validateInputData(invalidData), InvalidFormatException);
}

TEST_F(ShardAggregatorValidationTest, TestInvalidAggregationName) {
  auto invalidMap = folly::parseJson(
      fbpcf::io::read(baseDir_ + "invalid_aggregation_name.json"));
  auto invalidData = std::vector<folly::dynamic>({invalidMap});
  EXPECT_THROW(validateInputData(invalidData), InvalidFormatException);
}

// Tests for new validation function -- ad object format
TEST_F(ShardAggregatorValidationTest, AdObjectTestValidMeasurementInput) {
  auto validMap = std::make_shared<private_measurement::AggMetrics>(
      private_measurement::AggMetrics::fromDynamic(folly::parseJson(
          fbpcf::io::read(baseDir_ + "valid_measurement_shard.json"))));
  auto validData =
      std::vector<std::shared_ptr<private_measurement::AggMetrics>>({validMap});
  validateInputDataAggMetrics(validData, "ad_object");
}

TEST_F(ShardAggregatorValidationTest, AdObjectTestValidPCMInput) {
  auto invalidMap = std::make_shared<private_measurement::AggMetrics>(
      private_measurement::AggMetrics::fromDynamic(folly::parseJson(
          fbpcf::io::read(baseDir_ + "invalid_pcm_shard.json"))));
  auto invalidData =
      std::vector<std::shared_ptr<private_measurement::AggMetrics>>(
          {invalidMap});
  EXPECT_THROW(
      validateInputDataAggMetrics(invalidData, "ad_object"),
      InvalidFormatException);
}

TEST_F(ShardAggregatorValidationTest, AdObjectTestInvalidInputLift) {
  auto invalidMap = std::make_shared<private_measurement::AggMetrics>(
      private_measurement::AggMetrics::fromDynamic(folly::parseJson(
          fbpcf::io::read(baseDir_ + "valid_lift_input.json"))));
  auto invalidData =
      std::vector<std::shared_ptr<private_measurement::AggMetrics>>(
          {invalidMap});

  EXPECT_THROW(
      validateInputDataAggMetrics(invalidData, "ad_object"),
      InvalidFormatException);
}

TEST_F(ShardAggregatorValidationTest, AdObjectTestInvalidInputBadStructure) {
  auto invalidMap = std::make_shared<private_measurement::AggMetrics>(
      private_measurement::AggMetrics::fromDynamic(folly::parseJson(
          fbpcf::io::read(baseDir_ + "invalid_bad_structure.json"))));
  auto invalidData =
      std::vector<std::shared_ptr<private_measurement::AggMetrics>>(
          {invalidMap});

  EXPECT_THROW(
      validateInputDataAggMetrics(invalidData, "ad_object"),
      InvalidFormatException);
}

TEST_F(ShardAggregatorValidationTest, AdObjectTestInvalidInputEmptyMap0) {
  auto invalidMap = std::make_shared<private_measurement::AggMetrics>(
      private_measurement::AggMetrics::fromDynamic(folly::parseJson(
          fbpcf::io::read(baseDir_ + "invalid_empty_map_0.json"))));
  auto invalidData =
      std::vector<std::shared_ptr<private_measurement::AggMetrics>>(
          {invalidMap});

  EXPECT_THROW(
      validateInputDataAggMetrics(invalidData, "ad_object"),
      InvalidFormatException);
}

TEST_F(ShardAggregatorValidationTest, AdObjectTestInvalidInputEmptyMap1) {
  auto invalidMap = std::make_shared<private_measurement::AggMetrics>(
      private_measurement::AggMetrics::fromDynamic(folly::parseJson(
          fbpcf::io::read(baseDir_ + "invalid_empty_map_1.json"))));
  auto invalidData =
      std::vector<std::shared_ptr<private_measurement::AggMetrics>>(
          {invalidMap});

  EXPECT_THROW(
      validateInputDataAggMetrics(invalidData, "ad_object"),
      InvalidFormatException);
}

TEST_F(ShardAggregatorValidationTest, AdObjectTestInvalidAggregationName) {
  auto invalidMap = std::make_shared<private_measurement::AggMetrics>(
      private_measurement::AggMetrics::fromDynamic(folly::parseJson(
          fbpcf::io::read(baseDir_ + "invalid_aggregation_name.json"))));
  auto invalidData =
      std::vector<std::shared_ptr<private_measurement::AggMetrics>>(
          {invalidMap});

  EXPECT_THROW(
      validateInputDataAggMetrics(invalidData, "ad_object"),
      InvalidFormatException);
}

// Tests for new validation function -- lift
TEST_F(ShardAggregatorValidationTest, LiftTestValidLiftInput) {
  auto validMap = std::make_shared<private_measurement::AggMetrics>(
      private_measurement::AggMetrics::fromDynamic(folly::parseJson(
          fbpcf::io::read(baseDir_ + "valid_lift_input.json"))));
  auto validData =
      std::vector<std::shared_ptr<private_measurement::AggMetrics>>({validMap});
  validateInputDataAggMetrics(validData, "lift");
}

TEST_F(ShardAggregatorValidationTest, LiftTestInvalidAdObjectInput) {
  auto invalidMap = std::make_shared<private_measurement::AggMetrics>(
      private_measurement::AggMetrics::fromDynamic(folly::parseJson(
          fbpcf::io::read(baseDir_ + "valid_measurement_shard.json"))));
  auto invalidData =
      std::vector<std::shared_ptr<private_measurement::AggMetrics>>(
          {invalidMap});

  EXPECT_THROW(
      validateInputDataAggMetrics(invalidData, "lift"), InvalidFormatException);
}

TEST_F(ShardAggregatorValidationTest, LiftTestInvalidInputEmptyMap) {
  auto invalidMap = std::make_shared<private_measurement::AggMetrics>(
      private_measurement::AggMetrics::fromDynamic(folly::parseJson(
          fbpcf::io::read(baseDir_ + "invalid_empty_map_0.json"))));
  auto invalidData =
      std::vector<std::shared_ptr<private_measurement::AggMetrics>>(
          {invalidMap});

  EXPECT_THROW(
      validateInputDataAggMetrics(invalidData, "lift"), InvalidFormatException);
}

TEST_F(ShardAggregatorValidationTest, LiftTestValidInputEmptyCohortMetrics) {
  auto validMap = std::make_shared<private_measurement::AggMetrics>(
      private_measurement::AggMetrics::fromDynamic(folly::parseJson(
          fbpcf::io::read(baseDir_ + "valid_lift_no_cohort_metrics.json"))));
  auto validData =
      std::vector<std::shared_ptr<private_measurement::AggMetrics>>({validMap});

  validateInputDataAggMetrics(validData, "lift");
}

TEST_F(ShardAggregatorValidationTest, LiftTestInvalidInputEmptyMetrics) {
  auto invalidMap = std::make_shared<private_measurement::AggMetrics>(
      private_measurement::AggMetrics::fromDynamic(folly::parseJson(
          fbpcf::io::read(baseDir_ + "invalid_lift_no_metrics.json"))));
  auto invalidData =
      std::vector<std::shared_ptr<private_measurement::AggMetrics>>(
          {invalidMap});

  EXPECT_THROW(
      validateInputDataAggMetrics(invalidData, "lift"), InvalidFormatException);
}

TEST_F(ShardAggregatorValidationTest, LiftTestInvalidInputBadCohortMetric) {
  auto invalidMap = std::make_shared<private_measurement::AggMetrics>(
      private_measurement::AggMetrics::fromDynamic(folly::parseJson(
          fbpcf::io::read(baseDir_ + "invalid_lift_fake_cohort_metric.json"))));
  auto invalidData =
      std::vector<std::shared_ptr<private_measurement::AggMetrics>>(
          {invalidMap});

  EXPECT_THROW(
      validateInputDataAggMetrics(invalidData, "lift"), InvalidFormatException);
}

TEST_F(ShardAggregatorValidationTest, LiftTestInvalidInputBadMetric) {
  auto invalidMap = std::make_shared<private_measurement::AggMetrics>(
      private_measurement::AggMetrics::fromDynamic(folly::parseJson(
          fbpcf::io::read(baseDir_ + "invalid_lift_fake_metric.json"))));
  auto invalidData =
      std::vector<std::shared_ptr<private_measurement::AggMetrics>>(
          {invalidMap});

  EXPECT_THROW(
      validateInputDataAggMetrics(invalidData, "lift"), InvalidFormatException);
}

TEST_F(ShardAggregatorValidationTest, LiftTestInvalidInputMissingCohortMetric) {
  auto invalidMap = std::make_shared<private_measurement::AggMetrics>(
      private_measurement::AggMetrics::fromDynamic(
          folly::parseJson(fbpcf::io::read(
              baseDir_ + "invalid_lift_missing_cohort_metric.json"))));
  auto invalidData =
      std::vector<std::shared_ptr<private_measurement::AggMetrics>>(
          {invalidMap});

  EXPECT_THROW(
      validateInputDataAggMetrics(invalidData, "lift"), InvalidFormatException);
}

TEST_F(ShardAggregatorValidationTest, LiftTestInvalidInputMissingMetric) {
  auto invalidMap = std::make_shared<private_measurement::AggMetrics>(
      private_measurement::AggMetrics::fromDynamic(folly::parseJson(
          fbpcf::io::read(baseDir_ + "invalid_lift_missing_metric.json"))));
  auto invalidData =
      std::vector<std::shared_ptr<private_measurement::AggMetrics>>(
          {invalidMap});

  EXPECT_THROW(
      validateInputDataAggMetrics(invalidData, "lift"), InvalidFormatException);
}

TEST_F(ShardAggregatorValidationTest, LiftTestInvalidInputExtraCohortMetric) {
  auto invalidMap = std::make_shared<private_measurement::AggMetrics>(
      private_measurement::AggMetrics::fromDynamic(
          folly::parseJson(fbpcf::io::read(
              baseDir_ + "invalid_lift_extra_cohort_metric.json"))));
  auto invalidData =
      std::vector<std::shared_ptr<private_measurement::AggMetrics>>(
          {invalidMap});

  EXPECT_THROW(
      validateInputDataAggMetrics(invalidData, "lift"), InvalidFormatException);
}

TEST_F(ShardAggregatorValidationTest, LiftTestInvalidInputExtraMetric) {
  auto invalidMap = std::make_shared<private_measurement::AggMetrics>(
      private_measurement::AggMetrics::fromDynamic(folly::parseJson(
          fbpcf::io::read(baseDir_ + "invalid_lift_extra_metric.json"))));
  auto invalidData =
      std::vector<std::shared_ptr<private_measurement::AggMetrics>>(
          {invalidMap});

  EXPECT_THROW(
      validateInputDataAggMetrics(invalidData, "lift"), InvalidFormatException);
}
} // namespace measurement::private_attribution
