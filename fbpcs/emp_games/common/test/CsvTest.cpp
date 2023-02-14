/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <gtest/gtest.h>
#include <fstream>
#include "folly/Format.h"
#include "folly/Random.h"

#include "fbpcs/emp_games/common/Csv.h"
#include "fbpcs/emp_games/common/TestUtil.h"

namespace private_measurement {
class CsvTest : public ::testing::Test {
 private:
 protected:
  void SetUp() override {}
};

const std::vector<std::string> EXPECTED_HEADER = {
    "id",
    "field1",
    "field2",
    "field3"};

const std::vector<std::vector<std::string>> EXPECTED_VALUES = {
    {"1", "foo", "bubba", "gas"},
    {"2", "trio", "[1,2,3]", "[4,5,6]"}};

static void cleanup(std::string file_to_delete) {
  remove(file_to_delete.c_str());
}

TEST_F(CsvTest, TestSplitByCommaNotSupportInnerBrackets) {
  std::string inputStr =
      " 43feaeeecd7b2fe2ae2e26d917b6477d , 1 , 0 , 1600000192   ";
  std::vector<std::string> expOutput = {
      "43feaeeecd7b2fe2ae2e26d917b6477d", "1", "0", "1600000192"};
  auto output = csv::splitByComma(inputStr, false);
  EXPECT_EQ(expOutput, output);
}

TEST_F(CsvTest, TestSplitByCommaSupportInnerBrackets) {
  std::string inputStr =
      "  c4ca4238a0b923820dcc509a6f75849b,  [0, 0, 1600000330, 1600000594],  [0, 0, 71, 71] ";
  std::vector<std::string> expOutput = {
      "c4ca4238a0b923820dcc509a6f75849b",
      "[0,0,1600000330,1600000594]",
      "[0,0,71,71]"};
  auto output = csv::splitByComma(inputStr, true);
  EXPECT_EQ(expOutput, output);
}

TEST_F(CsvTest, TestReadCsv) {
  std::string baseDir = test_util::getBaseDirFromPath(__FILE__);
  std::string inputPath = baseDir + "test_data/input.csv";

  std::vector<std::string> headerInput;
  bool headerRead = false;
  std::vector<std::vector<std::string>> results;

  csv::readCsv(
      inputPath,
      [&headerInput, &headerRead, &results](
          const std::vector<std::string>& header,
          const std::vector<std::string>& values) {
        if (!headerRead) {
          headerInput = header;
        }

        results.push_back(values);
      });

  EXPECT_EQ(headerInput, EXPECTED_HEADER);
  EXPECT_EQ(results, EXPECTED_VALUES);
}

TEST_F(CsvTest, TestWriteCsv) {
  std::string baseDir = test_util::getBaseDirFromPath(__FILE__);
  std::string outputPath = folly::sformat(
      "{}test_data/output_{}.csv", baseDir, folly::Random::secureRand64());

  csv::writeCsv(outputPath, EXPECTED_HEADER, EXPECTED_VALUES);

  std::vector<std::string> headerInput;
  bool headerRead = false;
  std::vector<std::vector<std::string>> results;

  csv::readCsv(
      outputPath,
      [&headerInput, &headerRead, &results](
          const std::vector<std::string>& header,
          const std::vector<std::string>& values) {
        if (!headerRead) {
          headerInput = header;
        }

        results.push_back(values);
      });

  EXPECT_EQ(headerInput, EXPECTED_HEADER);
  EXPECT_EQ(results, EXPECTED_VALUES);

  cleanup(outputPath);
}

} // namespace private_measurement
