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

TEST(UnionPIDDataPreparerTest, ColumnCountTest) {
  std::vector<std::string> lines = {
      "id_,id_1,id_2,aaa,bbb",
      "123,456,789,abc,def",
      "111,,,aaa,bbb",
      "999,888,,aaa,bbb",
      ",777,,aaa,bbb",
      ",666,555,aaa,bbb"};
  std::string expected{"123,456\n111\n999,888\n777\n666,555\n"};
  std::filesystem::path inpath{tmpnam(nullptr)};
  std::filesystem::path outpath{tmpnam(nullptr)};
  writeLinesToFile(inpath, lines);

  UnionPIDDataPreparer preparer{inpath, outpath, "/tmp/", 2};
  preparer.prepare();
  validateFileContents(expected, outpath);
}

TEST(UnionPIDDataPreparerTest, DuplicateHandlingTest) {
  std::vector<std::string> lines = {
      "id_,id_1,id_2,aaa,bbb",
      "123,456,789,abc,def",
      "123,,,aaa,bbb",
      "999,888,,aaa,bbb",
      ",456,,aaa,bbb",
      "666,777,888,aaa,bbb"};
  std::string expected{"123,456,789\n999,888\n"};
  std::filesystem::path inpath{tmpnam(nullptr)};
  std::filesystem::path outpath{tmpnam(nullptr)};
  writeLinesToFile(inpath, lines);

  UnionPIDDataPreparer preparer{inpath, outpath, "/tmp/", 3};
  preparer.prepare();
  validateFileContents(expected, outpath);
}

TEST(UnionPIDDataPreparerTest, IdSwapInputValidationWithMaxOne) {
  std::vector<std::string> lines = {
      "id_,id_1,id_2,opportunity_timestamp,test_flag",
      "123,111,999,100,1",
      "123,222,888,120,1",
      "456,333,777,150,0",
      "456,333,777,160,1",
      "789,333,666,170,0",
      "789,,555,180,0",
      ",,789,190,0"};
  std::string expected{"123\n456\n789\n"};
  std::filesystem::path inpath{tmpnam(nullptr)};
  std::filesystem::path outpath{tmpnam(nullptr)};
  writeLinesToFile(inpath, lines);

  UnionPIDDataPreparer preparer{inpath, outpath, "/tmp/", 1};
  preparer.prepare();
  validateFileContents(expected, outpath);
}

TEST(UnionPIDDataPreparerTest, IdSwapInputValidationWithMaxTwo) {
  std::vector<std::string> lines = {
      "id_,id_1,id_2,opportunity_timestamp,test_flag",
      "123,111,999,100,1",
      "123,222,888,120,1",
      "456,333,777,150,0",
      "456,333,777,160,1",
      "789,333,666,170,0",
      "789,,555,180,0",
      ",,789,190,0"};
  std::string expected{"123,111\n456,333\n789,555\n"};
  std::filesystem::path inpath{tmpnam(nullptr)};
  std::filesystem::path outpath{tmpnam(nullptr)};
  writeLinesToFile(inpath, lines);

  UnionPIDDataPreparer preparer{inpath, outpath, "/tmp/", 2};
  preparer.prepare();
  validateFileContents(expected, outpath);
}

TEST(UnionPIDDataPreparerTest, IdSwapInputValidationWithMaxThree) {
  std::vector<std::string> lines = {
      "id_,id_1,id_2,opportunity_timestamp,test_flag",
      "123,111,999,100,1",
      "123,222,888,120,1",
      "456,333,777,150,0",
      "456,333,777,160,1",
      "789,333,666,200,0",
      "789,555,,200,0",
      ",789,,200,0"};
  std::string expected{"123,111,999\n456,333,777\n789,555\n"};
  std::filesystem::path inpath{tmpnam(nullptr)};
  std::filesystem::path outpath{tmpnam(nullptr)};
  writeLinesToFile(inpath, lines);

  UnionPIDDataPreparer preparer{inpath, outpath, "/tmp/", 3};
  preparer.prepare();
  validateFileContents(expected, outpath);
}

TEST(UnionPIDDataPreparerTest, IdSwapInputValidationWithMaxFour) {
  std::vector<std::string> lines = {
      "id_,id_1,id_2,opportunity_timestamp,test_flag",
      "123,111,999,100,1",
      "123,222,888,120,1",
      "456,333,777,150,0",
      "456,333,777,160,1",
      "789,333,666,200,0",
      "789,555,,200,0",
      ",,789,200,0"};
  std::string expected{"123,111,999\n456,333,777\n789,555\n"};
  std::filesystem::path inpath{tmpnam(nullptr)};
  std::filesystem::path outpath{tmpnam(nullptr)};
  writeLinesToFile(inpath, lines);

  UnionPIDDataPreparer preparer{inpath, outpath, "/tmp/", 4};
  preparer.prepare();
  validateFileContents(expected, outpath);
}

TEST(UnionPIDDataPreparerTest, LiftIdSpineInputValidationWithMaxTwo) {
  std::vector<std::string> lines = {
      "id_,id_2,id_3,event_timestamp,value",
      "123,456,789,128,105",
      ",456,789,126,103",
      ",,789,127,104",
      ",,789,125,102",
  };
  std::string expected{"123,456\n789\n"};
  std::filesystem::path inpath{tmpnam(nullptr)};
  std::filesystem::path outpath{tmpnam(nullptr)};
  writeLinesToFile(inpath, lines);

  UnionPIDDataPreparer preparer{inpath, outpath, "/tmp/", 2};
  preparer.prepare();
  validateFileContents(expected, outpath);
}

TEST(UnionPIDDataPreparerTest, LiftIdSpineInputValidationWithMaxThree) {
  std::vector<std::string> lines = {
      "id_,id_2,id_3,event_timestamp,value",
      "123,456,789,128,105",
      ",456,789,126,103",
      ",,789,127,104",
      ",,789,125,102",
  };
  std::string expected{"123,456,789\n"};
  std::filesystem::path inpath{tmpnam(nullptr)};
  std::filesystem::path outpath{tmpnam(nullptr)};
  writeLinesToFile(inpath, lines);

  UnionPIDDataPreparer preparer{inpath, outpath, "/tmp/", 3};
  preparer.prepare();
  validateFileContents(expected, outpath);
}
} // namespace measurement::pid
