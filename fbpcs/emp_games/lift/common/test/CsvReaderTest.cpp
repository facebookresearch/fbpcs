/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <stdexcept>
#include <string>

#include <gtest/gtest.h>

#include "fbpcs/emp_games/lift/common/CsvReader.h"

using namespace df;

TEST(CsvReaderDetail, Split) {
  std::vector<std::string> expected{"123", "456", "789"};
  EXPECT_EQ(expected, detail::split("123,456,789"));

  std::vector<std::string> expected2{"[1,2,3]", "456", "789"};
  EXPECT_EQ(expected2, detail::split("[1,2,3],456,789"));

  std::vector<std::string> expected3{"[1,2,3]", "[4,5,6]", "789"};
  EXPECT_EQ(expected3, detail::split("[1,2,3],[4,5,6],789"));

  std::vector<std::string> expected4{"[1,2,3,4,5,6,7,8,9]"};
  EXPECT_EQ(expected4, detail::split("[1,2,3,4,5,6,7,8,9]"));

  // Missing trailing ']'
  EXPECT_THROW(detail::split("[1,2,3,4,5,6,7,8,9"), std::out_of_range);
}
