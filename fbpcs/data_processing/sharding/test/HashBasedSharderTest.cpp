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
TEST(HashBasedSharderTest, TestGetShardFor) {
  HashBasedSharder sharder{"unused", {/* unused */}, 123, ""};
  std::string key = "abcd";
  EXPECT_EQ(sharder.getShardFor(key, 123), 25);
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
  std::vector<std::string> expected0{
      "9BX9ClsYtFj3L8N023K3mJnw1vemIGqenY5vfAY0/cg=,1,2,3"};
  std::vector<std::string> expected1{};

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
  std::vector<std::string> expected0{
      "9BX9ClsYtFj3L8N023K3mJnw1vemIGqenY5vfAY0/cg=,bSRNJ92+ML97JRfp1lEvqssXNCX+lI2T/HQtHRTkBk4=,1,2,3"};
  std::vector<std::string> expected1{};

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
      "9BX9ClsYtFj3L8N023K3mJnw1vemIGqenY5vfAY0/cg=,1,2,3", // first abcd line
      "9BX9ClsYtFj3L8N023K3mJnw1vemIGqenY5vfAY0/cg=,4,5,6", // second abcd line
  };

  std::vector<std::string> expected1{
      "id_,a,b,c",
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
      "9BX9ClsYtFj3L8N023K3mJnw1vemIGqenY5vfAY0/cg=,,1,2,3",
      "9BX9ClsYtFj3L8N023K3mJnw1vemIGqenY5vfAY0/cg=,ZGCVov/c63+N2Swslf6pY6pWsNzS1IkXKVi+lmAD6yU=,4,5,6",
  };
  std::vector<std::string> expected1{
      "id_email,id_phone,a,b,c",
      ",bSRNJ92+ML97JRfp1lEvqssXNCX+lI2T/HQtHRTkBk4=,7,8,9",
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
      "9BX9ClsYtFj3L8N023K3mJnw1vemIGqenY5vfAY0/cg=,,1,2,3",
      "9BX9ClsYtFj3L8N023K3mJnw1vemIGqenY5vfAY0/cg=,ZGCVov/c63+N2Swslf6pY6pWsNzS1IkXKVi+lmAD6yU=,4,5,6",
  };
  std::vector<std::string> expected1{
      "id_email,id_phone,a,b,c",
      ",bSRNJ92+ML97JRfp1lEvqssXNCX+lI2T/HQtHRTkBk4=,7,8,9",
  };
  data_processing::test_utils::expectFileRowsEqual(
      outputPaths.at(0), expected0);
  data_processing::test_utils::expectFileRowsEqual(
      outputPaths.at(1), expected1);
}

} // namespace data_processing::sharder
