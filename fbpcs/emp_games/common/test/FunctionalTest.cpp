/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <gtest/gtest.h>

#include <tuple>
#include <vector>

#include "../Functional.h"

namespace private_measurement::functional {

TEST(FunctionalTest, TestZipApplyBasic) {
  std::vector<int64_t> v{1, 2, 3, 4, 5};
  auto f = [](auto n) { return n * n; };
  std::vector<int64_t> expected{1, 4, 9, 16, 25};
  auto actual = zip_apply(f, v.begin(), v.end());

  EXPECT_EQ(expected, actual);
}

TEST(FunctionalTest, TestZipApplyAdvancedInputType) {
  std::vector<int64_t> v1{1, 2, 3, 4, 5};
  std::vector<int64_t> v2{11, 22, 33, 44, 55};
  std::vector<int64_t> v3{10, 20, 30, 40, 50};
  auto f = [](auto n1, auto n2, auto n3) { return n1 + n2 - n3; };
  std::vector<int64_t> expected{2, 4, 6, 8, 10};
  auto actual = zip_apply(f, v1.begin(), v1.end(), v2.begin(), v3.begin());

  EXPECT_EQ(expected, actual);
}

TEST(FunctionalTest, TestZipApplyAdvancedOutputType) {
  std::vector<int64_t> v{1, 2, 3, 4, 5};
  auto f = [](auto n) { return std::make_tuple(n, n + 1); };
  std::vector<std::tuple<int64_t, int64_t>> expected{
      std::make_tuple(1, 2),
      std::make_tuple(2, 3),
      std::make_tuple(3, 4),
      std::make_tuple(4, 5),
      std::make_tuple(5, 6)};
  auto actual = zip_apply(f, v.begin(), v.end());

  EXPECT_EQ(expected, actual);
}
} // namespace private_measurement::functional
