/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <string>
#include <vector>

#include <gtest/gtest.h>

#include <fbpcf/mpc/EmpGame.h>

#include "fbpcs/emp_games/lift/calculator/LiftDataFrameBuilder.h"
#include "fbpcs/emp_games/lift/calculator/LiftInputData.h"
#include "fbpcs/emp_games/lift/common/Column.h"
#include "fbpcs/emp_games/lift/common/DataFrame.h"

using namespace private_lift;

class MockLiftDataFrameBuilderForAlice : public LiftDataFrameBuilder {
 public:
  MockLiftDataFrameBuilderForAlice() : LiftDataFrameBuilder{"", 3} {}

  df::DataFrame buildNew() const final {
    df::DataFrame res;

    // clang-format off

    // Try to align these in a nice human-readable way to look like an
    // actual dataframe
    res.get<int64_t>("opportunity_timestamp") = {  111,     0,   222,   333};
    res.get<int64_t>("test_population") =       {    1,     0,     0,     1};
    res.get<int64_t>("control_population") =    {    0,     0,     1,     0};
    res.get<int64_t>("breakdown_id") =          {    1,     0,     0,     1};
    res.get<int64_t>("num_impressions") =       {    5,     0,     0,     1};
    res.get<int64_t>("num_clicks") =            {    2,     0,     0,     0};
    res.get<int64_t>("total_spend") =           {  100,     0,     0,   200};

    // clang-format on

    return res;
  }
  int64_t expectedGroupCount = 2;
  std::size_t expectedSize = 4;
  std::vector<df::Column<bool>> expectedBitmasks = {
      {false, true, true, false},
      {true, false, false, true}};
};

class MockLiftDataFrameBuilderForBob : public LiftDataFrameBuilder {
 public:
  MockLiftDataFrameBuilderForBob() : LiftDataFrameBuilder{"", 3} {}

  df::DataFrame buildNew() const final {
    df::DataFrame res;

    // clang-format off

    // Try to align these in a nice human-readable way to look like an
    // actual dataframe
    res.get<std::string>("id_") =                       {            "abc",       "def",         "ghi"};
    res.get<std::vector<int64_t>>("event_timestamps") = {{ 100,  200, 300}, {0, 0, 125}, {0,  150, 250}};
    res.get<std::vector<int64_t>>("values") =           {{  10,   20,  30}, {0, 0,  12}, {0,   15,  25}};
    res.get<std::vector<int64_t>>("values_squared") =   {{3600, 2500, 900}, {0, 0, 144}, {0, 1600, 625}};
    res.get<int64_t>("cohort_id") =                     {                0,           1,             2};

    // clang-format on

    return res;
  }

  int64_t expectedGroupCount = 3;
  std::size_t expectedSize = 3;
  std::vector<df::Column<bool>> expectedBitmasks = {
      {true, false, false},
      {false, true, false},
      {false, false, true}};
};

TEST(LiftInputDataTest, CalculateGroupCount) {
  MockLiftDataFrameBuilderForAlice mockAlice;
  LiftInputData alice{mockAlice, fbpcf::Party::Alice};

  EXPECT_EQ(mockAlice.expectedGroupCount, alice.getGroupCount());

  MockLiftDataFrameBuilderForBob mockBob;
  LiftInputData bob{mockBob, fbpcf::Party::Bob};

  EXPECT_EQ(mockBob.expectedGroupCount, bob.getGroupCount());
}

TEST(LiftInputDataTest, CalculateBitmasks) {
  MockLiftDataFrameBuilderForAlice mockAlice;
  LiftInputData alice{mockAlice, fbpcf::Party::Alice};

  for (std::size_t i = 0; i < mockAlice.expectedBitmasks.size(); ++i) {
    auto& expected = mockAlice.expectedBitmasks.at(i);
    auto& actual = alice.getBitmaskFor(i);
    EXPECT_EQ(expected, actual);
  }

  MockLiftDataFrameBuilderForBob mockBob;
  LiftInputData bob{mockBob, fbpcf::Party::Bob};

  for (std::size_t i = 0; i < mockBob.expectedBitmasks.size(); ++i) {
    auto& expected = mockBob.expectedBitmasks.at(i);
    auto& actual = bob.getBitmaskFor(i);
    EXPECT_EQ(expected, actual);
  }
}

TEST(LiftInputData, CalculateSize) {
  MockLiftDataFrameBuilderForAlice mockAlice;
  LiftInputData alice{mockAlice, fbpcf::Party::Alice};

  EXPECT_EQ(mockAlice.expectedSize, alice.size());

  MockLiftDataFrameBuilderForBob mockBob;
  LiftInputData bob{mockBob, fbpcf::Party::Bob};

  EXPECT_EQ(mockBob.expectedSize, bob.size());
}
