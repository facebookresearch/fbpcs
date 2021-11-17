/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <string>
#include <vector>

#include <gtest/gtest.h>

#include <folly/Random.h>

#include "fbpcs/data_processing/sharding/HashBasedSharder.h"
#include "fbpcs/data_processing/test_utils/FileIOTestUtils.h"

namespace data_processing::sharder {

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

TEST(HashBasedSharderTest, TestBytesToIntAdvanced) {
  // Don't throw std::out_of_range if bytes is empty
  std::vector<unsigned char> bytes{};
  EXPECT_EQ(0, detail::bytesToInt(bytes));

  // If bytes are missing, we still copy the bytes array from the "start" so
  // this is equivalent to the test in TestBytesToIntSimple. In other words,
  // this is like copying [0b 0000 0001 0000 0000 [implicit 0000 0000 0000 0000]
  // Hopefully this is clear -- the lower two bytes were never "overridden" so
  // they still contain zero.
  std::vector<unsigned char> bytes2{1, 0};
  EXPECT_EQ(1 << 24, detail::bytesToInt(bytes2));
}

TEST(HashBasedSharderTest, TestGetShardFor) {
  // Assuming toBytes and bytesToInt have been tested elsewhere, this is a
  // straightforward modulo operation.
  std::string key = "abcd";
  auto integerValue = detail::bytesToInt(detail::toBytes(key));
  EXPECT_EQ(detail::getShardFor(key, 123), integerValue % 123);
  // Anything % 1 should be zero
  EXPECT_EQ(detail::getShardFor(key, 1), 0);
}

TEST(HashBasedSharderTest, TestShardNoHmacKey) {
  std::vector<std::string> rows{
      "id_,a,b,c", "abcd,1,2,3", "abcd,4,5,6", "defg,7,8,9", "hijk,0,0,0",
  };

  std::string inputPath = "/tmp/HashBasedSharderTestShardInput" +
                          std::to_string(folly::Random::secureRand64());
  data_processing::test_utils::writeVecToFile(rows, inputPath);
  // TODO: Would be great to mock out inputstream/outputstream stuff
  auto randStart = folly::Random::secureRand64();
  std::vector<std::string> outputPaths{
      "/tmp/HashBasedSharderTestShardOutput" + std::to_string(randStart),
      "/tmp/HashBasedSharderTestShardOutput" + std::to_string(randStart + 1),
  };
  HashBasedSharder sharder{inputPath, outputPaths, 123, ""};
  sharder.shard();

  std::vector<std::string> expected0{
      "id_,a,b,c",
      "abcd,1,2,3",
      "abcd,4,5,6",
  };
  std::vector<std::string> expected1{
      "id_,a,b,c",
      "defg,7,8,9",
      "hijk,0,0,0",
  };
  data_processing::test_utils::expectFileRowsEqual(outputPaths.at(0),
                                                   expected0);
  data_processing::test_utils::expectFileRowsEqual(outputPaths.at(1),
                                                   expected1);
}

TEST(HashBasedSharderTest, TestShardWithHmacKey) {
  std::vector<std::string> rows{
      "id_,a,b,c", "abcd,1,2,3", "abcd,4,5,6", "defg,7,8,9", "hijk,0,0,0",
  };
  std::string hmacKey = "abcd1234";

  std::string inputPath = "/tmp/HashBasedSharderTestShardInput" +
                          std::to_string(folly::Random::secureRand64());
  data_processing::test_utils::writeVecToFile(rows, inputPath);
  // TODO: Would be great to mock out inputstream/outputstream stuff
  auto randStart = folly::Random::secureRand64();
  std::vector<std::string> outputPaths{
      "/tmp/HashBasedSharderTestShardOutput" + std::to_string(randStart),
      "/tmp/HashBasedSharderTestShardOutput" + std::to_string(randStart + 1),
  };
  HashBasedSharder sharder{inputPath, outputPaths, 123, hmacKey};
  sharder.shard();

  // HMAC was applied offline, which is how we got these expected lines
  // HMAC_SHA256(CAST(id AS VARBINARY), FROM_BASE64(hmacKey)) in Presto is a
  // good way to generate more of these given our I/O specification.
  std::vector<std::string> expected0{
      "id_,a,b,c",
      "bSRNJ92+ML97JRfp1lEvqssXNCX+lI2T/HQtHRTkBk4=,7,8,9", // defg line
      "ZGCVov/c63+N2Swslf6pY6pWsNzS1IkXKVi+lmAD6yU=,0,0,0", // hijk line
  };
  std::vector<std::string> expected1{
      "id_,a,b,c",
      "9BX9ClsYtFj3L8N023K3mJnw1vemIGqenY5vfAY0/cg=,1,2,3", // first abcd line
      "9BX9ClsYtFj3L8N023K3mJnw1vemIGqenY5vfAY0/cg=,4,5,6", // second abcd line
  };
  data_processing::test_utils::expectFileRowsEqual(outputPaths.at(0),
                                                   expected0);
  data_processing::test_utils::expectFileRowsEqual(outputPaths.at(1),
                                                   expected1);
}
} // namespace data_processing::sharder
