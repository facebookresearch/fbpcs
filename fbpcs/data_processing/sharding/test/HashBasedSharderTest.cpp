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

TEST(HashBasedSharderTest, TestBytesToIntSimple) {
  // First a very simple test (but still important for endianness correctness!)
  std::vector<unsigned char> bytes{0, 0, 0, 1};
  EXPECT_EQ(1, detail::bytesToInt(bytes));

  // Assuming network byte order, big-endian 0x1 | 0x0 | 0x0 | 0x0
  // is equivalent to integer 16777216 (2^24). In binary, we recognize this
  // number as 0b 0000 0001 0000 0000 0000 0000 0000 0000
  std::vector<unsigned char> bytes2{1, 0, 0, 0};
  EXPECT_EQ(1 << 24, detail::bytesToInt(bytes2));
}
