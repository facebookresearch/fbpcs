/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <cstdint>
#include <cstdio>
#include <filesystem>
#include <fstream>
#include <string>
#include <vector>

#include <gtest/gtest.h>

#include "UnionPIDDataPreparer.h"

static void writeLinesToFile(
    const std::filesystem::path& path,
    const std::vector<std::string>& lines) {
  std::ofstream f{path};
  for (const auto& line : lines) {
    f << line << '\n';
  }
}

static std::string readFile(const std::filesystem::path& path) {
  std::cerr << "Read file: " << path << '\n';
  std::ifstream f{path};
  std::stringstream buf;
  buf << f.rdbuf();
  return buf.str();
}

static void validateFileContents(
    const std::string& expected,
    const std::filesystem::path& path,
    bool shouldAssert = false) {
  auto actual = readFile(path);
  if (shouldAssert) {
    ASSERT_EQ(expected, actual);
  } else {
    EXPECT_EQ(expected, actual);
  }
}

static void validateRowCounts(
    const std::int32_t& expected,
    const std::filesystem::path& path,
    bool shouldAssert = false) {
  std::ifstream f{path};
  std::string line;
  std::int32_t actual = 0;
  while (getline(f, line)) {
    actual++;
  }
  if (shouldAssert) {
    ASSERT_EQ(expected, actual);
  } else {
    EXPECT_EQ(expected, actual);
  }
}

namespace measurement::pid {

TEST(UnionPIDDataPreparerTest, InvalidHeader) {
  std::vector<std::string> lines = {
      "aaa,bbb,ccc"
      "123,456,789",
      "111,222,333"};
  std::filesystem::path inpath{tmpnam(nullptr)};
  std::filesystem::path outpath{tmpnam(nullptr)};
  writeLinesToFile(inpath, lines);

  UnionPIDDataPreparer preparer{inpath, outpath, "/tmp/"};
  ASSERT_DEATH(preparer.prepare(), ".*column missing from input header.*");
}

TEST(UnionPIDDataPreparerTest, RowLengthMismatch) {
  std::vector<std::string> lines = {
      "id_,aaa,bbb,ccc", "123,456,789", "111,222,333,444"};
  std::filesystem::path inpath{tmpnam(nullptr)};
  std::filesystem::path outpath{tmpnam(nullptr)};
  writeLinesToFile(inpath, lines);

  UnionPIDDataPreparer preparer{inpath, outpath, "/tmp/"};
  ASSERT_DEATH(
      preparer.prepare(), ".*Mismatch between header and row at index 0.*");
}

TEST(UnionPIDDataPreparerTest, DuplicateIdsNotAdded) {
  std::vector<std::string> lines = {
      "id_,aaa,bbb",
      "123,456,789",
      "123,456,789",
      "111,222,333",
      "111,222,333",
      "999,888,777"};
  std::string expected{"123\n111\n999\n"};
  std::filesystem::path inpath{tmpnam(nullptr)};
  std::filesystem::path outpath{tmpnam(nullptr)};
  writeLinesToFile(inpath, lines);

  UnionPIDDataPreparer preparer{inpath, outpath, "/tmp/"};
  auto res = preparer.prepare();
  validateFileContents(expected, outpath);
  EXPECT_EQ(2, res.duplicateIdCount);
}

TEST(UnionPIDDataPreparerTest, ValidTest) {
  std::vector<std::string> lines = {
      "id_,aaa,bbb", "123,456,789", "111,222,333", "999,888,777"};
  std::string expected{"123\n111\n999\n"};
  std::filesystem::path inpath{tmpnam(nullptr)};
  std::filesystem::path outpath{tmpnam(nullptr)};
  writeLinesToFile(inpath, lines);

  UnionPIDDataPreparer preparer{inpath, outpath, "/tmp/"};
  preparer.prepare();
  validateFileContents(expected, outpath);
}

TEST(UnionPIDDataPreparerTest, RowCountTest) {
  std::vector<std::string> lines = {"id_"};
  std::int32_t rowCountExpected = 1;
  std::filesystem::path inpath{tmpnam(nullptr)};
  std::filesystem::path outpath{tmpnam(nullptr)};
  writeLinesToFile(inpath, lines);

  UnionPIDDataPreparer preparer{inpath, outpath, "/tmp/"};
  preparer.prepare();
  validateRowCounts(rowCountExpected, outpath);
}

} // namespace measurement::pid
