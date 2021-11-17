/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <string>
#include <vector>

#include <gtest/gtest.h>

#include "fbpcs/data_processing/sharding/GenericSharder.h"

namespace data_processing::sharder {

TEST(GenericSharderTest, TestStripQuotes) {
  std::string noQuotes{"hello world"};
  std::string quoted{"\"hello world\""};
  std::string quotedMiddle{"hello \"world\""};

  detail::stripQuotes(noQuotes);
  EXPECT_EQ(noQuotes, "hello world");
  detail::stripQuotes(quoted);
  EXPECT_EQ(quoted, "hello world");
  detail::stripQuotes(quotedMiddle);
  EXPECT_EQ(quotedMiddle, "hello world");
}

TEST(GenericSharderTest, TestGenOutputPaths) {
  std::string basePath = "/tmp";
  std::size_t start = 0;
  std::size_t end = 4;
  std::vector<std::string> expected{"/tmp_0", "/tmp_1", "/tmp_2", "/tmp_3"};
  EXPECT_EQ(GenericSharder::genOutputPaths(basePath, start, end), expected);
}
} // namespace data_processing::sharder
