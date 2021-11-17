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

/**
 * A class to make GenericSharder concrete for testing purposes.
 */
class GenericSharderTest final : public GenericSharder {
  using GenericSharder::GenericSharder;
  void shard() const final { /* empty */ }
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
} // namespace data_processing::sharder
