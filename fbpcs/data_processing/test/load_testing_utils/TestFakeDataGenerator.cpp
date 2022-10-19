/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "fbpcs/data_processing/load_testing_utils/FakeDataGenerator.h"

#include <string>
#include <vector>

#include <gtest/gtest.h>

constexpr int64_t SEED = 10182022;

TEST(TestFakeDataGeneratorParams, withOpportunityRate) {
  const std::vector<std::string> header{"a", "b", "c"};
  FakeDataGeneratorParams params{header};
  params.withOpportunityRate(1.23);
  // TODO: Add validation for valid ranges of params
  EXPECT_DOUBLE_EQ(params.opportunityRate, 1.23);
}

TEST(TestFakeDataGeneratorParams, withTestRate) {
  const std::vector<std::string> header{"a", "b", "c"};
  FakeDataGeneratorParams params{header};
  params.withTestRate(4.56);
  // TODO: Add validation for valid ranges of params
  EXPECT_DOUBLE_EQ(params.testRate, 4.56);
}

TEST(TestFakeDataGeneratorParams, withPurchaseRate) {
  const std::vector<std::string> header{"a", "b", "c"};
  FakeDataGeneratorParams params{header};
  params.withPurchaseRate(7.89);
  // TODO: Add validation for valid ranges of params
  EXPECT_DOUBLE_EQ(params.purchaseRate, 7.89);
}

TEST(TestFakeDataGeneratorParams, withMinTs) {
  const std::vector<std::string> header{"a", "b", "c"};
  FakeDataGeneratorParams params{header};
  params.withMinTs(123);
  EXPECT_EQ(params.minTs, 123);
}

TEST(TestFakeDataGeneratorParams, withMaxTs) {
  const std::vector<std::string> header{"a", "b", "c"};
  FakeDataGeneratorParams params{header};
  params.withMaxTs(456);
  EXPECT_EQ(params.maxTs, 456);
}

TEST(TestFakeDataGeneratorParams, withMinValue) {
  const std::vector<std::string> header{"a", "b", "c"};
  FakeDataGeneratorParams params{header};
  params.withMinValue(123);
  EXPECT_EQ(params.minValue, 123);
}

TEST(TestFakeDataGeneratorParams, withMaxValue) {
  const std::vector<std::string> header{"a", "b", "c"};
  FakeDataGeneratorParams params{header};
  params.withMaxValue(456);
  EXPECT_EQ(params.maxValue, 456);
}

TEST(TestFakeDataGeneratorParams, withShouldUseComplexIds) {
  const std::vector<std::string> header{"a", "b", "c"};
  FakeDataGeneratorParams params{header};
  params.withShouldUseComplexIds(false);
  EXPECT_FALSE(params.shouldUseComplexIds);
}

TEST(TestFakeDataGeneratorParams, withNumConversions) {
  const std::vector<std::string> header{"a", "b", "c"};
  FakeDataGeneratorParams params{header};
  params.withNumConversions(111);
  EXPECT_EQ(params.numConversions, 111);
}

TEST(TestFakeDataGenerator, genOneRowForPublisher) {
  const std::vector<std::string> header{
      "id_", "opportunity_timestamp", "test_flag", "breakdown_id"};
  FakeDataGeneratorParams params{header};
  FakeDataGenerator g{params, SEED};

  auto row = g.genOneRow();
  EXPECT_EQ(row, "");
}

TEST(TestFakeDataGenerator, genOneRowForPartner) {
  const std::vector<std::string> header{"id_", "event_timestamp", "value"};
  FakeDataGeneratorParams params{header};
  FakeDataGenerator g{params, SEED};

  auto row = g.genOneRow();
  EXPECT_EQ(row, "");
}
TEST(TestFakeDataGenerator, genOneRowForInvalidHeader) {
  const std::vector<std::string> header{"id_", "bad_column_name"};
  FakeDataGeneratorParams params{header};
  FakeDataGenerator g{params, SEED};

  auto row = g.genOneRow();
  EXPECT_EQ(row, "");
}
