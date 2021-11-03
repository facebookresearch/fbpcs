/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <stdexcept>
#include <vector>

#include <gtest/gtest.h>

#include "fbpcs/emp_games/lift/common/Column.h"
#include "fbpcs/emp_games/lift/common/DataFrame.h"

using namespace df;

TEST(DataFrameTest, CreateBasicDataFrame) {
  DataFrame df;
  Column<int64_t> c1{1, 2, 3};
  df.get<int64_t>("intCol1") = c1;

  Column<int64_t> c2{4, 5, 6};
  df.get<int64_t>("intCol2") = std::move(c2);

  df.get<int64_t>("intCol3") = {7, 8, 9};

  df.get<std::string>("stringCol") = {"a", "b", "c"};
  df.get<std::vector<int64_t>>("intVecCol") = {{1, 2}, {3, 4}, {5, 6}};
}

TEST(DataFrameTest, MissingColumn) {
  DataFrame df;
  df.get<int64_t>("abc") = {1, 2, 3};
  // Throw because we're accessing a missing column
  EXPECT_THROW(df.at<int64_t>("def"), std::out_of_range);
  // Throw because we're accessing the wrong type
  EXPECT_THROW(df.at<std::string>("abc"), BadTypeException);
}

TEST(DataFrameTest, CheckType) {
  DataFrame::TypeInfo string{std::type_index(typeid(std::string)), "string"};
  DataFrame::TypeInfo int64{std::type_index(typeid(int64_t)), "int64_t"};
  DataFrame::TypeInfo string2{std::type_index(typeid(std::string)), "string"};

  EXPECT_NO_THROW(DataFrame::checkType(string, string2));
  EXPECT_THROW(DataFrame::checkType(string, int64), BadTypeException);
}

TEST(DataFrameTest, DropColumn) {
  DataFrame df;
  std::vector<int64_t> vI{1, 2, 3};
  std::vector<std::string> vS{"a", "b", "c"};

  df.get<int64_t>("intCol") = vI;
  Column cI(vI);
  df.get<std::string>("stringCol") = vS;
  Column cS(vS);

  EXPECT_EQ(df.at<int64_t>("intCol"), cI);
  EXPECT_EQ(df.at<std::string>("stringCol"), cS);

  df.drop<int64_t>("intCol");
  EXPECT_THROW(df.at<int64_t>("intCol"), std::out_of_range);
}
