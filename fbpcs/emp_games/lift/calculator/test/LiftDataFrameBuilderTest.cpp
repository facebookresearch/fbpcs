/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <string>
#include <vector>

#include <gtest/gtest.h>

#include "fbpcs/emp_games/lift/calculator/LiftDataFrameBuilder.h"
#include "fbpcs/emp_games/lift/common/Column.h"
#include "fbpcs/emp_games/lift/common/DataFrame.h"

constexpr int64_t kConversionCap = 2;

using namespace private_lift;

class LiftDataFrameBuilderTest : public ::testing::Test {
 public:
  void SetUp() override {
    // clang-format off

    // Try to align these in a nice human-readable way to look like an actual dataframe
    dfPublisher.get<std::string>("id_") =         {"abc", "def", "ghi"};
    dfPublisher.get<int64_t>("opportunity") =     {    1,     1,     0};
    dfPublisher.get<int64_t>("test_flag") =       {    1,     0,     0};
    dfPublisher.get<int64_t>("breakdown_id") =    {    0,     1,     0};
    dfPublisher.get<int64_t>("num_impressions") = {    5,     0,     0};
    dfPublisher.get<int64_t>("num_clicks") =      {    2,     0,     0};
    dfPublisher.get<int64_t>("total_spend") =     {  100,     0,     0};

    // It gets a bit messy with nested vectors, but hopefully it's halfway readable
    dfPartner.get<std::string>("id_") =                       {          "abc",       "def",        "ghi"};
    dfPartner.get<std::vector<int64_t>>("event_timestamps") = {{100, 200, 300}, {0, 0, 125}, {0, 150, 250}};
    dfPartner.get<std::vector<int64_t>>("values") =           {{ 10,  20,  30}, {0, 0,  12}, {0,  15,  25}};
    dfPartner.get<int64_t>("cohort_id") =                     {              0,           1,             2};

    // clang-format on

    expectedTestPopulation = {1, 0, 0};
    expectedControlPopulation = {0, 1, 0};
    expectedEventTimestampsCapped = {{100, 200}, {0, 0}, {0, 150}};
    expectedValuesCapped = {{10, 20}, {0, 0}, {0, 15}};
    expectedValuesSquaredPrecomputed = {{900, 400}, {0, 0}, {225, 225}};
  }

  df::DataFrame dfPublisher;
  df::DataFrame dfPartner;
  df::Column<int64_t> expectedTestPopulation;
  df::Column<int64_t> expectedControlPopulation;
  df::Column<std::vector<int64_t>> expectedEventTimestampsCapped;
  df::Column<std::vector<int64_t>> expectedValuesCapped;
  df::Column<std::vector<int64_t>> expectedValuesSquaredPrecomputed;
};

TEST_F(LiftDataFrameBuilderTest, ApplyLiftRules) {
  LiftDataFrameBuilder builder{"", kConversionCap};

  builder.applyLiftRules(dfPublisher);
  EXPECT_EQ(dfPublisher.at<int64_t>("test_population"), expectedTestPopulation);
  EXPECT_EQ(
      dfPublisher.at<int64_t>("control_population"), expectedControlPopulation);

  auto keysPublisher = dfPublisher.keys();
  for (const auto& key : LiftDataFrameBuilder::getNecessaryColumnsForLift()) {
    keysPublisher.erase(key);
  }
  // We expect |keys - necessaryKeys| = empty set
  EXPECT_TRUE(keysPublisher.empty());

  builder.applyLiftRules(dfPartner);
  EXPECT_EQ(
      dfPartner.at<std::vector<int64_t>>("event_timestamps"),
      expectedEventTimestampsCapped);
  EXPECT_EQ(dfPartner.at<std::vector<int64_t>>("values"), expectedValuesCapped);
  EXPECT_EQ(
      dfPartner.at<std::vector<int64_t>>("values_squared"),
      expectedValuesSquaredPrecomputed);

  auto keysPartner = dfPartner.keys();
  for (const auto& key : LiftDataFrameBuilder::getNecessaryColumnsForLift()) {
    keysPartner.erase(key);
  }
  // We expect |keys - necessaryKeys| = empty set
  EXPECT_TRUE(keysPartner.empty());

  // Lastly, check that we didn't add columns to irrelevant DataFrames
  // This is slightly different from the above check, because all of these
  // columns are *necessary*, just for the other party's df (not ours).
  EXPECT_EQ(keysPublisher.find("event_timestamps"), keysPublisher.end());
  EXPECT_EQ(keysPublisher.find("values"), keysPublisher.end());
  EXPECT_EQ(keysPublisher.find("values_squared"), keysPublisher.end());
  EXPECT_EQ(keysPartner.find("test_population"), keysPartner.end());
  EXPECT_EQ(keysPartner.find("control_population"), keysPartner.end());
}

TEST_F(LiftDataFrameBuilderTest, AddTestControlPopulationColumns) {
  LiftDataFrameBuilder builder{"", kConversionCap};

  builder.addTestControlPopulationColumns(dfPublisher);
  EXPECT_EQ(dfPublisher.at<int64_t>("test_population"), expectedTestPopulation);
  EXPECT_EQ(
      dfPublisher.at<int64_t>("control_population"), expectedControlPopulation);

  // For the partner, we wouldn't expect anything to happen at all since the
  // opportunity and test flag columns are not present
  builder.addTestControlPopulationColumns(dfPartner);
  auto keys = dfPartner.keys();
  EXPECT_EQ(keys.find("test_population"), keys.end());
  EXPECT_EQ(keys.find("control_population"), keys.end());
}

TEST_F(LiftDataFrameBuilderTest, ApplyConversionCap) {
  LiftDataFrameBuilder builder{"", kConversionCap};

  // For the publisher, we wouldn't expect anything to happen at all since the
  // event_timestamps and values flag columns are not present
  builder.applyConversionCap(dfPublisher);
  auto keys = dfPublisher.keys();
  EXPECT_EQ(keys.find("event_timestamps"), keys.end());
  EXPECT_EQ(keys.find("values"), keys.end());

  builder.applyConversionCap(dfPartner);
  EXPECT_EQ(
      dfPartner.at<std::vector<int64_t>>("event_timestamps"),
      expectedEventTimestampsCapped);
  EXPECT_EQ(dfPartner.at<std::vector<int64_t>>("values"), expectedValuesCapped);
}

TEST_F(LiftDataFrameBuilderTest, PrecomputeValuesSquared) {
  LiftDataFrameBuilder builder{"", kConversionCap};

  // NOTE: For coherence with the test fixture's expected capping, we must first
  // call the applyConversionCap function here again.
  builder.applyConversionCap(dfPublisher);
  builder.applyConversionCap(dfPartner);

  // For the publisher, we wouldn't expect anything to happen at all since the
  // event_timestamps and values flag columns are not present
  builder.precomputeValuesSquared(dfPublisher);
  auto keys = dfPublisher.keys();
  EXPECT_EQ(keys.find("values_squared"), keys.end());

  builder.precomputeValuesSquared(dfPartner);
  EXPECT_EQ(
      dfPartner.at<std::vector<int64_t>>("values_squared"),
      expectedValuesSquaredPrecomputed);
}

TEST_F(LiftDataFrameBuilderTest, DropUnnecessaryColumns) {
  LiftDataFrameBuilder builder{"", kConversionCap};

  builder.dropUnnecessaryColumns(dfPublisher);
  auto keysPublisher = dfPublisher.keys();

  std::vector<std::string> diffPublisher;
  for (const auto& key : LiftDataFrameBuilder::getNecessaryColumnsForLift()) {
    keysPublisher.erase(key);
  }
  // We expect |keys - necessaryKeys| = empty set
  EXPECT_TRUE(keysPublisher.empty());

  builder.dropUnnecessaryColumns(dfPartner);
  auto keysPartner = dfPartner.keys();
  for (const auto& key : LiftDataFrameBuilder::getNecessaryColumnsForLift()) {
    keysPartner.erase(key);
  }
  // We expect |keys - necessaryKeys| = empty set
  EXPECT_TRUE(keysPartner.empty());
}
