/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <stdexcept>

#include <gtest/gtest.h>

#include "fbpcs/emp_games/lift/common/Column.h"

using namespace df;

class Foo {
public:
  Foo(int64_t a, int64_t b) : a_{a}, b_{b} {}

  friend bool operator==(const Foo &f1, const Foo &f2) {
    return f1.a_ == f2.a_ && f1.b_ == f2.b_;
  }

private:
  int64_t a_;
  int64_t b_;
};

TEST(Constructor, Default) {
  Column<int64_t> c;
  c.push_back(1);
  c.push_back(2);
  c.push_back(3);

  ASSERT_EQ(c.size(), 3);
  EXPECT_EQ(c.at(0), 1);
  EXPECT_EQ(c.at(1), 2);
  EXPECT_EQ(c.at(2), 3);
}

TEST(Constructor, DefaultFilled) {
  Column<int64_t> c(4, 5);

  ASSERT_EQ(c.size(), 4);
  EXPECT_EQ(c.at(0), 5);
  EXPECT_EQ(c.at(1), 5);
  EXPECT_EQ(c.at(2), 5);
  EXPECT_EQ(c.at(3), 5);
}

TEST(Constructor, FromIterator) {
  std::vector<int64_t> vec{4, 5, 6};
  Column<int64_t> c(vec.begin(), vec.end());

  ASSERT_EQ(c.size(), 3);
  EXPECT_EQ(c.at(0), 4);
  EXPECT_EQ(c.at(1), 5);
  EXPECT_EQ(c.at(2), 6);
}

TEST(Constructor, CopyVector) {
  std::vector<int64_t> vec{7, 8, 9};
  Column<int64_t> c(vec);

  ASSERT_EQ(c.size(), 3);
  EXPECT_EQ(c.at(0), 7);
  EXPECT_EQ(c.at(1), 8);
  EXPECT_EQ(c.at(2), 9);
}

TEST(Constructor, FromVectorRValueReference) {
  std::vector<int64_t> vec{1, 3, 5};
  Column<int64_t> c(std::move(vec));

  ASSERT_EQ(c.size(), 3);
  EXPECT_EQ(c.at(0), 1);
  EXPECT_EQ(c.at(1), 3);
  EXPECT_EQ(c.at(2), 5);
}

TEST(Constructor, FromInitializerList) {
  Column<int64_t> c{2, 4, 6};
  ASSERT_EQ(c.size(), 3);
  EXPECT_EQ(c.at(0), 2);
  EXPECT_EQ(c.at(1), 4);
  EXPECT_EQ(c.at(2), 6);
}

TEST(Constructor, FromColumnReference) {
  Column<int64_t> from{9, 8, 7};
  Column<int64_t> c(from);

  ASSERT_EQ(c.size(), 3);
  EXPECT_EQ(c.at(0), 9);
  EXPECT_EQ(c.at(1), 8);
  EXPECT_EQ(c.at(2), 7);
}

TEST(Constructor, FromColumnRValueReference) {
  Column<int64_t> from{6, 5, 4};
  Column<int64_t> c(std::move(from));

  ASSERT_EQ(c.size(), 3);
  EXPECT_EQ(c.at(0), 6);
  EXPECT_EQ(c.at(1), 5);
  EXPECT_EQ(c.at(2), 4);
}

// Copy assignment constructor given std::vector
TEST(CopyAssignmentConstructor, FromVectorReference) {
  std::vector<int64_t> from{3, 2, 1};
  Column<int64_t> c = from;

  ASSERT_EQ(c.size(), 3);
  EXPECT_EQ(c.at(0), 3);
  EXPECT_EQ(c.at(1), 2);
  EXPECT_EQ(c.at(2), 1);
}
// Copy constructor given std::vector&&
TEST(CopyAssignmentConstructor, FromVectorRValueReference) {
  std::vector<int64_t> from{3, 5, 7};
  Column<int64_t> c = std::move(from);

  ASSERT_EQ(c.size(), 3);
  EXPECT_EQ(c.at(0), 3);
  EXPECT_EQ(c.at(1), 5);
  EXPECT_EQ(c.at(2), 7);
}
// Copy constructor given Column
TEST(CopyAssignmentConstructor, FromColumnReference) {
  Column<int64_t> from{4, 6, 8};
  Column<int64_t> c = from;

  ASSERT_EQ(c.size(), 3);
  EXPECT_EQ(c.at(0), 4);
  EXPECT_EQ(c.at(1), 6);
  EXPECT_EQ(c.at(2), 8);
}
// Copy constructor given Column&&
TEST(CopyAssignmentConstructor, FromColumnRValueReference) {
  std::vector<int64_t> from{5, 7, 9};
  Column<int64_t> c = std::move(from);
  Column<int64_t> c2 = std::move(c);

  ASSERT_EQ(c2.size(), 3);
  EXPECT_EQ(c2.at(0), 5);
  EXPECT_EQ(c2.at(1), 7);
  EXPECT_EQ(c2.at(2), 9);
}
// Copy constructor given std::initializer_list
TEST(CopyAssignmentConstructor, FromInitializerList) {
  Column<int64_t> c = {2, 4, 6, 8, 10};

  ASSERT_EQ(c.size(), 5);
  EXPECT_EQ(c.at(0), 2);
  EXPECT_EQ(c.at(1), 4);
  EXPECT_EQ(c.at(2), 6);
  EXPECT_EQ(c.at(3), 8);
  EXPECT_EQ(c.at(4), 10);
}

TEST(ColumnFunctionality, At) {
  Column<int64_t> c{1, 2, 3, 4, 5};
  EXPECT_EQ(c.at(0), 1);
  EXPECT_EQ(c.at(1), 2);
  EXPECT_EQ(c.at(2), 3);
  EXPECT_EQ(c.at(3), 4);
  EXPECT_EQ(c.at(4), 5);
  EXPECT_THROW(c.at(5), std::out_of_range);
}

TEST(ColumnFunctionality, Empty) {
  Column<int64_t> c;
  EXPECT_TRUE(c.empty());

  c.push_back(1);
  c.push_back(2);
  c.push_back(3);
  EXPECT_FALSE(c.empty());
}

TEST(ColumnFunctionality, Size) {
  Column<int64_t> c;
  EXPECT_EQ(c.size(), 0);

  c.push_back(1);
  c.push_back(2);
  c.push_back(3);
  EXPECT_EQ(c.size(), 3);
}

TEST(ColumnFunctionality, EmplaceBack) {
  Column<Foo> c;
  Foo f(123, 456);
  c.emplace_back(123, 456);

  ASSERT_EQ(c.size(), 1);
  EXPECT_EQ(c.at(0), f);
}

TEST(ColumnFunctionality, ComparisonOperators) {
  Column<int64_t> c1{1, 2, 3};
  Column<int64_t> c2{1, 2, 3};
  Column<int64_t> c3{4, 5, 6};

  EXPECT_EQ(c1, c2);
  EXPECT_EQ(c2, c1);
  EXPECT_NE(c1, c3);
}

TEST(FunctionalTest, Apply) {
  Column<int64_t> c1{1, 2, 3};
  Column<int64_t> c2{1, 4, 9};

  c1.apply([](int64_t &v) { v *= v; });
  EXPECT_EQ(c1, c2);

  std::vector<int64_t> vec;
  std::vector<int64_t> vecExpected{1, 4, 9};
  c2.apply([&vec](int64_t v) { vec.push_back(v); });
  EXPECT_EQ(vec, vecExpected);
}

TEST(FunctionalTest, Map) {
  Column<int64_t> c1{1, 2, 3};
  auto c2 = c1.map([](int64_t v) { return v + 1; });
  Column<int64_t> expected{2, 3, 4};

  EXPECT_EQ(c2, expected);

  // Also showing off that you can pass a function that takes a const-ref
  c1.mapInPlace([](const int64_t &v) { return v * 2; });
  Column<int64_t> expected2{2, 4, 6};

  EXPECT_EQ(c1, expected2);
}

TEST(FunctionalTest, Reduce) {
  Column<int64_t> c{10, 20, 30};
  // Basic test
  auto sum = c.reduce([](int64_t acc, int64_t v) { return acc + v; });
  EXPECT_EQ(sum, 60);

  // Use non-default accumulator
  auto product = c.reduce([](int64_t acc, int64_t v) { return acc * v; }, 1);
  EXPECT_EQ(product, 6000);

  // Empty column, simple accumulator
  Column<int64_t> c2;
  auto x = c2.reduce([](int64_t acc, int64_t v) { return acc + v; }, 123);
  EXPECT_EQ(x, 123);

  // Empty column, no accumulator
  EXPECT_THROW(c2.reduce([](int64_t acc, int64_t v) { return acc + v; }),
               std::out_of_range);
}
