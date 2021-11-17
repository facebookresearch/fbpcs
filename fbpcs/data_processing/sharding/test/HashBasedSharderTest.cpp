/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <string>
#include <vector>

#include <gtest/gtest.h>

#include "fbpcs/data_processing/sharding/HashBasedSharder.h"

using namespace data_processing::sharder;

TEST(HashBasedSharderTest, TestToBytes) {
  std::string key = "abcd";
  std::vector<unsigned char> expected{
      static_cast<unsigned char>('a'), static_cast<unsigned char>('b'),
      static_cast<unsigned char>('c'), static_cast<unsigned char>('d')};
  EXPECT_EQ(detail::toBytes(key), expected);
}
