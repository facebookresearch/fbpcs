/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <string>
#include <vector>

#include <fbpcf/io/api/BufferedWriter.h>
#include <fbpcf/io/api/FileWriter.h>
#include <folly/Random.h>
#include <gtest/gtest.h>

#include "fbpcs/data_processing/sharding/HashBasedSharder.h"
#include "fbpcs/data_processing/test_utils/FileIOTestUtils.h"

namespace data_processing::sharder {
TEST(HashBasedSharderTest, TestToBytes) {
  std::string key = "abcd";
  std::vector<unsigned char> expected{
      static_cast<unsigned char>('a'),
      static_cast<unsigned char>('b'),
      static_cast<unsigned char>('c'),
      static_cast<unsigned char>('d')};
  EXPECT_EQ(detail::toBytes(key), expected);
}

TEST(HashBasedSharderTest, TestBytesToUInt64) {
  // ntohl reverse the byte order on a little-endian machine, and are no-ops on
  // big-endian machines.
  // big-endian 0x0000'0001'0000'0000 is equivalent to integer 1 << 32.
  std::vector<unsigned char> bytes{0, 0, 0, 1};
  EXPECT_EQ(1ll << 32, detail::bytesToUInt64(bytes));

  // Don't throw std::out_of_range if bytes is empty
  std::vector<unsigned char> bytes1{};
  // The big-endian is 0x0000'0000'0000'0000
  EXPECT_EQ(0, detail::bytesToUInt64(bytes1));

  // Assuming network byte order, big-endian 0x0100'0000'0000'0000 is equivalent
  // to integer 1 << 56.
  std::vector<unsigned char> bytes2{1, 0, 0, 0};
  EXPECT_EQ(1ll << 56, detail::bytesToUInt64(bytes2));

  // If bytes are missing, we still copy the bytes array from the "start".
  // big-endian 0x0100'0000'0000'0000 is equivalent
  // to integer 1 << 56.
  std::vector<unsigned char> bytes3{1, 0};
  EXPECT_EQ(1ll << 56, detail::bytesToUInt64(bytes3));

  // If bytes are missing, we still copy the bytes array from the "start". We
  // will trucncate the array if the size of array is large than 8 big-endian
  // 0x0100'0000'0000'0000 is equivalent to integer 1 << 56.
  std::vector<unsigned char> bytes4{1, 0, 0, 0, 0, 0, 0, 0, 0, 0};
  EXPECT_EQ(1ll << 56, detail::bytesToUInt64(bytes3));
}

TEST(HashBasedSharderTest, TestGetShardFor) {
  // Assuming toBytes and bytesToUInt64 have been tested elsewhere, this is a
  // straightforward modulo operation.
  HashBasedSharder sharder{"unused", {/* unused */}, 123, ""};
  std::string key = "abcd";
  auto integerValue = detail::bytesToUInt64(detail::toBytes(key));
  EXPECT_EQ(sharder.getShardFor(key, 123), integerValue % 123);
  // Anything % 1 should be zero
  EXPECT_EQ(sharder.getShardFor(key, 1), 0);
}

TEST(HashBasedSharderTest, TestShardLineNoHmacKey) {
  std::string line = "abcd,1,2,3";
  std::vector<std::unique_ptr<fbpcf::io::BufferedWriter>> streams(0);
  auto randStart = folly::Random::secureRand64();
  std::vector<std::string> outputPaths{
      "/tmp/HashBasedSharderTestShardOutput" + std::to_string(randStart),
      "/tmp/HashBasedSharderTestShardOutput" + std::to_string(randStart + 1),
  };
  auto fileWriter0 = std::make_unique<fbpcf::io::FileWriter>(outputPaths.at(0));
  auto fileWriter1 = std::make_unique<fbpcf::io::FileWriter>(outputPaths.at(1));

  streams.push_back(
      std::make_unique<fbpcf::io::BufferedWriter>(std::move(fileWriter0)));
  streams.push_back(
      std::make_unique<fbpcf::io::BufferedWriter>(std::move(fileWriter1)));

  HashBasedSharder sharder{"unused", outputPaths, 123, ""};
  std::vector<int32_t> idColumnIndices{0};
  sharder.shardLine(line, streams, idColumnIndices);

  // We can just reset the underlying unique_ptr to flush the writes to disk
  streams.at(0)->close();
  streams.at(1)->close();

  // We didn't write headers, so we expect to *just* have the written line
  std::vector<std::string> expected0{"abcd,1,2,3"};
  std::vector<std::string> expected1{};

  data_processing::test_utils::expectFileRowsEqual(
      outputPaths.at(0), expected0);
  data_processing::test_utils::expectFileRowsEqual(
      outputPaths.at(1), expected1);
}

TEST(HashBasedSharderTest, TestShardLineWithHmacKey) {
  std::string line = "abcd,1,2,3";
  std::vector<std::unique_ptr<fbpcf::io::BufferedWriter>> streams(0);
  auto randStart = folly::Random::secureRand64();
  std::vector<std::string> outputPaths{
      "/tmp/HashBasedSharderTestShardOutput" + std::to_string(randStart),
      "/tmp/HashBasedSharderTestShardOutput" + std::to_string(randStart + 1),
  };
  auto fileWriter0 = std::make_unique<fbpcf::io::FileWriter>(outputPaths.at(0));
  auto fileWriter1 = std::make_unique<fbpcf::io::FileWriter>(outputPaths.at(1));

  streams.push_back(
      std::make_unique<fbpcf::io::BufferedWriter>(std::move(fileWriter0)));
  streams.push_back(
      std::make_unique<fbpcf::io::BufferedWriter>(std::move(fileWriter1)));

  std::string hmacKey = "abcd1234";
  HashBasedSharder sharder{"unused", outputPaths, 123, hmacKey};
  std::vector<int32_t> idColumnIndices{0};
  sharder.shardLine(line, streams, idColumnIndices);

  // We can just reset the underlying unique_ptr to flush the writes to disk
  streams.at(0)->close();
  streams.at(1)->close();

  // We didn't write headers, so we expect to *just* have the written line
  std::vector<std::string> expected0{};
  std::vector<std::string> expected1{
      "9BX9ClsYtFj3L8N023K3mJnw1vemIGqenY5vfAY0/cg=,1,2,3"};

  data_processing::test_utils::expectFileRowsEqual(
      outputPaths.at(0), expected0);
  data_processing::test_utils::expectFileRowsEqual(
      outputPaths.at(1), expected1);
}

TEST(HashBasedSharderTest, TestShardMultiKeyLineWithHmacKey) {
  std::string line = "abcd,defg,1,2,3";
  std::vector<std::unique_ptr<fbpcf::io::BufferedWriter>> streams(0);
  auto randStart = folly::Random::secureRand64();
  std::vector<std::string> outputPaths{
      "/tmp/HashBasedSharderTestShardOutput" + std::to_string(randStart),
      "/tmp/HashBasedSharderTestShardOutput" + std::to_string(randStart + 1),
  };
  auto fileWriter0 = std::make_unique<fbpcf::io::FileWriter>(outputPaths.at(0));
  auto fileWriter1 = std::make_unique<fbpcf::io::FileWriter>(outputPaths.at(1));

  streams.push_back(
      std::make_unique<fbpcf::io::BufferedWriter>(std::move(fileWriter0)));
  streams.push_back(
      std::make_unique<fbpcf::io::BufferedWriter>(std::move(fileWriter1)));

  std::string hmacKey = "abcd1234";
  HashBasedSharder sharder{"unused", outputPaths, 123, hmacKey};
  std::vector<int32_t> idColumnIndices{0, 1};
  sharder.shardLine(line, streams, idColumnIndices);

  // We can just reset the underlying unique_ptr to flush the writes to disk
  streams.at(0)->close();
  streams.at(1)->close();

  // We didn't write headers, so we expect to *just* have the written line
  std::vector<std::string> expected0{};
  std::vector<std::string> expected1{
      "9BX9ClsYtFj3L8N023K3mJnw1vemIGqenY5vfAY0/cg=,bSRNJ92+ML97JRfp1lEvqssXNCX+lI2T/HQtHRTkBk4=,1,2,3"};

  data_processing::test_utils::expectFileRowsEqual(
      outputPaths.at(0), expected0);
  data_processing::test_utils::expectFileRowsEqual(
      outputPaths.at(1), expected1);
}

TEST(HashBasedSharderTest, TestShardNoHmacKey) {
  std::vector<std::string> rows{
      "id_,a,b,c",
      "abcd,1,2,3",
      "abcd,4,5,6",
      "defg,7,8,9",
      "hijk,0,0,0",
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
      "defg,7,8,9",
      "hijk,0,0,0",
  };
  std::vector<std::string> expected1{
      "id_,a,b,c",
  };
  data_processing::test_utils::expectFileRowsEqual(
      outputPaths.at(0), expected0);
  data_processing::test_utils::expectFileRowsEqual(
      outputPaths.at(1), expected1);
}

TEST(HashBasedSharderTest, TestShardWithHmacKey) {
  std::vector<std::string> rows{
      "id_,a,b,c",
      "abcd,1,2,3",
      "abcd,4,5,6",
      "defg,7,8,9",
      "hijk,0,0,0",
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
  };
  std::vector<std::string> expected1{
      "id_,a,b,c",
      "9BX9ClsYtFj3L8N023K3mJnw1vemIGqenY5vfAY0/cg=,1,2,3", // first abcd line
      "9BX9ClsYtFj3L8N023K3mJnw1vemIGqenY5vfAY0/cg=,4,5,6", // second abcd line
      "bSRNJ92+ML97JRfp1lEvqssXNCX+lI2T/HQtHRTkBk4=,7,8,9", // defg line
      "ZGCVov/c63+N2Swslf6pY6pWsNzS1IkXKVi+lmAD6yU=,0,0,0", // hijk line
  };
  data_processing::test_utils::expectFileRowsEqual(
      outputPaths.at(0), expected0);
  data_processing::test_utils::expectFileRowsEqual(
      outputPaths.at(1), expected1);
}

TEST(HashBasedSharderTest, TestShardMultiKeyWithHmacKey) {
  std::vector<std::string> rows{
      "id_email,id_phone,a,b,c",
      "abcd,,1,2,3",
      "abcd,hijk,4,5,6",
      ",defg,7,8,9",
      ",,0,0,0",
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
      "id_email,id_phone,a,b,c",
  };
  std::vector<std::string> expected1{
      "id_email,id_phone,a,b,c",
      "9BX9ClsYtFj3L8N023K3mJnw1vemIGqenY5vfAY0/cg=,,1,2,3", // abcd, line
      "9BX9ClsYtFj3L8N023K3mJnw1vemIGqenY5vfAY0/cg=,ZGCVov/c63+N2Swslf6pY6pWsNzS1IkXKVi+lmAD6yU=,4,5,6", // abcd,hijk line
      ",bSRNJ92+ML97JRfp1lEvqssXNCX+lI2T/HQtHRTkBk4=,7,8,9", // ,defg line
  };
  data_processing::test_utils::expectFileRowsEqual(
      outputPaths.at(0), expected0);
  data_processing::test_utils::expectFileRowsEqual(
      outputPaths.at(1), expected1);
}

TEST(HashBasedSharderTest, TestShardMultiKeyWithNullsQuotes) {
  std::vector<std::string> rows{
      "id_email,id_phone,a,b,c",
      "\"abcd\",null,1,2,3",
      "'abcd',\"hijk\",4,5,6",
      "null,'defg',7,8,9",
      "null,NULL,0,0,0",
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
      "id_email,id_phone,a,b,c",
  };
  std::vector<std::string> expected1{
      "id_email,id_phone,a,b,c",
      "9BX9ClsYtFj3L8N023K3mJnw1vemIGqenY5vfAY0/cg=,,1,2,3", // abcd, line
      "9BX9ClsYtFj3L8N023K3mJnw1vemIGqenY5vfAY0/cg=,ZGCVov/c63+N2Swslf6pY6pWsNzS1IkXKVi+lmAD6yU=,4,5,6", // abcd,hijk line
      ",bSRNJ92+ML97JRfp1lEvqssXNCX+lI2T/HQtHRTkBk4=,7,8,9", // ,defg line
  };
  data_processing::test_utils::expectFileRowsEqual(
      outputPaths.at(0), expected0);
  data_processing::test_utils::expectFileRowsEqual(
      outputPaths.at(1), expected1);
}

} // namespace data_processing::sharder
