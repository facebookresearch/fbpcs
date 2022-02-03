/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <memory>
#include <string>
#include <vector>

#include <gtest/gtest.h>

#include <folly/Random.h>

#include "fbpcs/data_processing/sharding/GenericSharder.h"
#include "fbpcs/data_processing/test_utils/FileIOTestUtils.h"

namespace data_processing::sharder {

/**
 * A class to make GenericSharder concrete for testing purposes.
 */
class GenericSharderTest final : public GenericSharder {
 public:
  using GenericSharder::GenericSharder;
  std::size_t getShardFor(
      const std::string& /* unused */,
      std::size_t /* unused */) final {
    return shardFor_;
  }

  void shardLine(
      std::string line,
      const std::vector<std::unique_ptr<std::ofstream>>& /* unused */) final {
    linesCalledWith_.push_back(line);
  }

  std::size_t shardFor_ = 123;
  std::vector<std::string> linesCalledWith_;
};

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

TEST(GenericSharderTest, TestDos2Unix) {
  std::string dosLine{"hello world\r\n"};
  std::string unixLine{"hello world\n"};
  std::string lineNoNewline{"hello world"};

  detail::dos2Unix(dosLine);
  EXPECT_EQ(dosLine, "hello world\n");
  detail::dos2Unix(unixLine);
  EXPECT_EQ(unixLine, "hello world\n");
  detail::dos2Unix(lineNoNewline);
  EXPECT_EQ(lineNoNewline, "hello world");
}

TEST(GenericSharderTest, TestGenOutputPaths) {
  std::string basePath = "/tmp";
  std::size_t start = 0;
  std::size_t end = 4;
  std::vector<std::string> expected{"/tmp_0", "/tmp_1", "/tmp_2", "/tmp_3"};
  EXPECT_EQ(GenericSharder::genOutputPaths(basePath, start, end), expected);
}

TEST(GenericSharderTest, TestGetInputPath) {
  std::vector<std::string> outputPaths{"/tmp_0", "/tmp_1", "/tmp_2", "/tmp_3"};
  int32_t logEveryN = 123;
  GenericSharderTest actual{"/tmp", outputPaths, logEveryN};
  EXPECT_EQ(actual.getInputPath(), "/tmp");
}

TEST(GenericSharderTest, TestGetLogRate) {
  std::vector<std::string> outputPaths{"/tmp_0", "/tmp_1", "/tmp_2", "/tmp_3"};
  int32_t logEveryN = 123;
  GenericSharderTest actual{"/tmp", outputPaths, logEveryN};
  EXPECT_EQ(actual.getLogRate(), 123);
}

TEST(GenericSharderTest, TestGetOutputPaths) {
  std::vector<std::string> outputPaths{"/tmp_0", "/tmp_1", "/tmp_2", "/tmp_3"};
  int32_t logEveryN = 123;
  GenericSharderTest actual{"/tmp", outputPaths, logEveryN};
  EXPECT_EQ(actual.getOutputPaths(), outputPaths);

  // Also test version based on genOutputPaths
  std::string basePath = "/tmp";
  std::size_t start = 0;
  std::size_t end = 4;
  GenericSharderTest actual2{"/tmp", basePath, start, end, logEveryN};
  EXPECT_EQ(actual2.getOutputPaths(), outputPaths);
}

TEST(GenericSharderTest, TestGetShardFor) {
  std::vector<std::string> outputPaths{"/tmp_0", "/tmp_1", "/tmp_2", "/tmp_3"};
  int32_t logEveryN = 123;
  GenericSharderTest actual{"/tmp", outputPaths, logEveryN};
  auto actualShard = actual.getShardFor("line", 999);
  EXPECT_EQ(actualShard, actual.shardFor_);
}

TEST(GenericSharderTest, TestShardLine) {
  // This test is just ensuring that internally, shardLine is being called for
  // each line of input except the header.
  auto randStart = folly::Random::secureRand64();
  std::string inputPath =
      "/tmp/GenericSharderTestShardLineInput" + std::to_string(randStart);
  std::vector<std::string> outputPaths{
      "/tmp/GenericSharderTestShardLineOutput" + std::to_string(randStart),
      "/tmp/GenericSharderTestShardLineOutput" + std::to_string(randStart + 1),
  };
  int32_t logEveryN = 123;
  GenericSharderTest actual{inputPath, outputPaths, logEveryN};
  std::vector<std::string> rows{
      "id_,a,b,c",
      "abcd,1,2,3",
      "abcd,4,5,6",
      "defg,7,8,9",
      "hijk,0,0,0",
  };
  data_processing::test_utils::writeVecToFile(rows, inputPath);
  actual.shard();
  // Should have been called on everything except the header
  std::vector<std::string> expected{
      "abcd,1,2,3",
      "abcd,4,5,6",
      "defg,7,8,9",
      "hijk,0,0,0",
  };
  EXPECT_EQ(actual.linesCalledWith_, expected);
}
} // namespace data_processing::sharder
