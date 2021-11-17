/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <string>

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
} // namespace data_processing::sharder
