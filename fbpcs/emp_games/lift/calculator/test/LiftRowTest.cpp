/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <vector>

#include <gtest/gtest.h>

#include "fbpcs/emp_games/lift/calculator/LiftRow.h"
#include "fbpcs/emp_games/lift/common/ColumnNameConstants.h"
#include "fbpcs/emp_games/lift/common/DataFrame.h"

using namespace private_lift;

df::DataFrame buildBasicDataFrame() {
  df::DataFrame dframe;
  // NOTE: For testing, we are setting *all* columns as int64_t
  // This allows us to test against specific constants instead of `true`
  // which could be harder to debug in case of an error. This is another benefit
  // of making BitType and IntType template parameters. Testing is easy!
  dframe.get<int64_t>(lift_columns::kOpportunityTimestamp) = {1, 2, 3};
  dframe.get<int64_t>(lift_columns::kTestPopulation) = {4, 5, 6};
  dframe.get<int64_t>(lift_columns::kControlPopulation) = {7, 8, 9};
  dframe.get<int64_t>(lift_columns::kReached) = {10, 11, 12};
  dframe.get<int64_t>(lift_columns::kPartnerRow) = {16, 17, 18};
  dframe.get<std::vector<int64_t>>(lift_columns::kEventTimestamps) = {
      {19}, {20}, {21}};
  dframe.get<std::vector<int64_t>>(lift_columns::kValues) = {{22}, {23}, {24}};
  dframe.get<std::vector<int64_t>>(lift_columns::kValuesSquared) = {
      {25}, {26}, {27}};
  return dframe;
}

TEST(LiftRowTest, FromDataFrameAllPresent) {
  auto dframe = buildBasicDataFrame();
  dframe.get<int64_t>(lift_columns::kBreakdownId) = {13, 14, 15};
  dframe.get<int64_t>(lift_columns::kCohortId) = {28, 29, 30};

  // First test will verify all the rows just to show it works
  auto row = LiftRow<int64_t, int64_t>::fromDataFrame(dframe, 0);
  EXPECT_EQ(*row.opportunityTimestamp, 1);
  EXPECT_EQ(*row.testPopulation, 4);
  EXPECT_EQ(*row.controlPopulation, 7);
  EXPECT_EQ(*row.reachedPopulation, 10);
  EXPECT_EQ(*row.breakdownId, 13);
  EXPECT_EQ(*row.partnerRow, 16);
  EXPECT_EQ((*row.eventTimestamps).at(0), 19);
  EXPECT_EQ((*row.values).at(0), 22);
  EXPECT_EQ((*row.valuesSquared).at(0), 25);
  EXPECT_EQ(*row.cohortId, 28);

  auto row2 = LiftRow<int64_t, int64_t>::fromDataFrame(dframe, 1);
  EXPECT_EQ(*row2.opportunityTimestamp, 2);
  EXPECT_EQ(*row2.testPopulation, 5);
  EXPECT_EQ(*row2.controlPopulation, 8);
  EXPECT_EQ(*row2.reachedPopulation, 11);
  EXPECT_EQ(*row2.breakdownId, 14);
  EXPECT_EQ(*row2.partnerRow, 17);
  EXPECT_EQ((*row2.eventTimestamps).at(0), 20);
  EXPECT_EQ((*row2.values).at(0), 23);
  EXPECT_EQ((*row2.valuesSquared).at(0), 26);
  EXPECT_EQ(*row2.cohortId, 29);

  auto row3 = LiftRow<int64_t, int64_t>::fromDataFrame(dframe, 2);
  EXPECT_EQ(*row3.opportunityTimestamp, 3);
  EXPECT_EQ(*row3.testPopulation, 6);
  EXPECT_EQ(*row3.controlPopulation, 9);
  EXPECT_EQ(*row3.reachedPopulation, 12);
  EXPECT_EQ(*row3.breakdownId, 15);
  EXPECT_EQ(*row3.partnerRow, 18);
  EXPECT_EQ((*row3.eventTimestamps).at(0), 21);
  EXPECT_EQ((*row3.values).at(0), 24);
  EXPECT_EQ((*row3.valuesSquared).at(0), 27);
  EXPECT_EQ(*row3.cohortId, 30);
}

TEST(LiftRowTest, FromDataFrameNoBreakdown) {
  auto dframe = buildBasicDataFrame();
  dframe.get<int64_t>(lift_columns::kCohortId) = {28, 29, 30};

  // Validate a couple columns
  auto row = LiftRow<int64_t, int64_t>::fromDataFrame(dframe, 0);
  EXPECT_EQ(*row.opportunityTimestamp, 1);
  EXPECT_EQ(row.breakdownId, nullptr);
  EXPECT_EQ(*row.partnerRow, 16);
  EXPECT_EQ(*row.cohortId, 28);
}

TEST(LiftRowTest, FromDataFrameNoCohort) {
  auto dframe = buildBasicDataFrame();
  dframe.get<int64_t>(lift_columns::kBreakdownId) = {13, 14, 15};

  // Validate a couple columns
  auto row = LiftRow<int64_t, int64_t>::fromDataFrame(dframe, 1);
  EXPECT_EQ(*row.opportunityTimestamp, 2);
  EXPECT_EQ(*row.breakdownId, 14);
  EXPECT_EQ(*row.partnerRow, 17);
  EXPECT_EQ(row.cohortId, nullptr);
}

TEST(LiftRowTest, FromDataFrameNoOptionalColumns) {
  auto dframe = buildBasicDataFrame();

  // Validate a couple columns
  auto row = LiftRow<int64_t, int64_t>::fromDataFrame(dframe, 1);
  EXPECT_EQ(*row.opportunityTimestamp, 2);
  EXPECT_EQ(row.breakdownId, nullptr);
  EXPECT_EQ(*row.partnerRow, 17);
  EXPECT_EQ(row.cohortId, nullptr);
}
