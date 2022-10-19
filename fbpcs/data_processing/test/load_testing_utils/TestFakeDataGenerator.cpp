/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <string>
#include <vector>

#include <gtest/gtest.h>

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

TEST(TestFakeDataGeneratorParams, withIncrementalityRate) {
  const std::vector<std::string> header{"a", "b", "c"};
  FakeDataGeneratorParams params{header};
  params.withIncrementalityRate(0.12);
  // TODO: Add validation for valid ranges of params
  EXPECT_DOUBLE_EQ(params.incrementalityRate, 0.12);
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

TEST(TestFakeDataGeneratorParams, withShouldUseMd5Ids) {
  const std::vector<std::string> header{"a", "b", "c"};
  FakeDataGeneratorParams params{header};
  params.withShouldUseMd5Ids(false);
  EXPECT_FALSE(params.shouldUseMd5Ids);
}

TEST(TestFakeDataGeneratorParams, withNumConversions) {
  const std::vector<std::string> header{"a", "b", "c"};
  FakeDataGeneratorParams params{header};
  params.withNumConversions(111);
  EXPECT_EQ(params.numConversions, 111);
}
