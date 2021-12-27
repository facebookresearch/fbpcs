/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <limits>
#include <string>
#include <vector>

#include <gtest/gtest.h>

#include <folly/Random.h>
#include <folly/String.h>

#include "fbpcs/data_processing/sharding/Sharding.h"
#include "fbpcs/data_processing/test_utils/FileIOTestUtils.h"

using namespace data_processing::sharder;

// clang-format off
static const std::vector<std::string> inputLines {
  "id_,test_flag,opportunity_timestamp,num_impressions,num_clicks,opportunity,total_spend",
  "0149BE3A4AB6B424CBDB47DB81F9544E9B3FDE7187399585819B589F9611E38,0,1600001404,2,0,1,300",
  "1060EE7494E81E82091D27CD77263A46B8836817A77626D2071349ED248619,0,1600002618,2,1,1,496",
  "126D9AA3EBCFFAD92F0A0505FDC40AE9AFE918962A1D8923CB153ACD1EBA732,1,1600002642,3,0,1,332",
  "1A3D4E3559532FC64CEBBF45E78E42AE9454C8ADAB256A755B4F59CAAD23419,0,0,0,0,0,0",
  "1CF4F4E2D2172AF2C5B5483158C230D16E207A5CA1CCF6BABAEBA140D062262F,0,1600002583,3,0,1,588",
  "20183696B042288AD04635742E365BD5A487BFB1605599F8ECDC39CC51FF47,1,1600001697,3,3,1,443",
  "224BF134C6CDEA319E9CB05E4ACA9AD9A979E497A6FBC79BAA0F25F8F6BFA5B,0,1600001934,3,3,1,451",
  "2499912C418FA70D3CA87BA47322977494B49E023B43C537C5E1D705DCF7A1,1,1600001587,1,1,1,111",
  "28645ED9AB4399584190C5476EFFDCC616A09459CBE7BBB63B639D064EEBA22,0,0,0,0,0,0",
  "2AFC8D27B59FBB5C74BBE7B2D46B659CD89438198B89EA178C987C616776251F,0,0,0,0,0,0",
  "2D97E35F26B6A234CAC415CBC2E13138CCC3513A108958679E2B8658FF927F,1,1600002534,1,1,1,322",
  "32B1722F75F3681FE8E4BD7DC283A7F95BB32443D5A3CFFA28177D9EFB5418,0,0,0,0,0,0",
  "34E56397B276A699CFBC5BE45331E26A243DAC9A3C78EAC3B837E6436D9E927,0,1600002365,1,0,1,666",
  "3A61C0F39C972A7766941D59282240A74168CE9FCF61F288CEF9642F4E89650,1,1600001758,3,3,1,114",
  "3CA35EA9FA1F1852CAA776CDEBDF8B0EF43922333AAD183FEF3632F37146131,0,0,0,0,0,0",
  "4E4EF28D8819F3585E28527577484CE124FA24E085522DE7A868CA4E0977A,1,1600002059,2,2,1,462",
};

static std::vector<std::vector<std::string>> expectedOutBasic {
  {
    "id_,test_flag,opportunity_timestamp,num_impressions,num_clicks,opportunity,total_spend",
    "0149BE3A4AB6B424CBDB47DB81F9544E9B3FDE7187399585819B589F9611E38,0,1600001404,2,0,1,300",
    "126D9AA3EBCFFAD92F0A0505FDC40AE9AFE918962A1D8923CB153ACD1EBA732,1,1600002642,3,0,1,332",
    "1CF4F4E2D2172AF2C5B5483158C230D16E207A5CA1CCF6BABAEBA140D062262F,0,1600002583,3,0,1,588",
    "224BF134C6CDEA319E9CB05E4ACA9AD9A979E497A6FBC79BAA0F25F8F6BFA5B,0,1600001934,3,3,1,451",
    "28645ED9AB4399584190C5476EFFDCC616A09459CBE7BBB63B639D064EEBA22,0,0,0,0,0,0",
    "2D97E35F26B6A234CAC415CBC2E13138CCC3513A108958679E2B8658FF927F,1,1600002534,1,1,1,322",
    "34E56397B276A699CFBC5BE45331E26A243DAC9A3C78EAC3B837E6436D9E927,0,1600002365,1,0,1,666",
    "3CA35EA9FA1F1852CAA776CDEBDF8B0EF43922333AAD183FEF3632F37146131,0,0,0,0,0,0",
  },
  {
    "id_,test_flag,opportunity_timestamp,num_impressions,num_clicks,opportunity,total_spend",
    "1060EE7494E81E82091D27CD77263A46B8836817A77626D2071349ED248619,0,1600002618,2,1,1,496",
    "1A3D4E3559532FC64CEBBF45E78E42AE9454C8ADAB256A755B4F59CAAD23419,0,0,0,0,0,0",
    "20183696B042288AD04635742E365BD5A487BFB1605599F8ECDC39CC51FF47,1,1600001697,3,3,1,443",
    "2499912C418FA70D3CA87BA47322977494B49E023B43C537C5E1D705DCF7A1,1,1600001587,1,1,1,111",
    "2AFC8D27B59FBB5C74BBE7B2D46B659CD89438198B89EA178C987C616776251F,0,0,0,0,0,0",
    "32B1722F75F3681FE8E4BD7DC283A7F95BB32443D5A3CFFA28177D9EFB5418,0,0,0,0,0,0",
    "3A61C0F39C972A7766941D59282240A74168CE9FCF61F288CEF9642F4E89650,1,1600001758,3,3,1,114",
    "4E4EF28D8819F3585E28527577484CE124FA24E085522DE7A868CA4E0977A,1,1600002059,2,2,1,462",
  }};

static std::vector<std::vector<std::string>> expectedOutPid {
  {
    "id_,test_flag,opportunity_timestamp,num_impressions,num_clicks,opportunity,total_spend",
    "1060EE7494E81E82091D27CD77263A46B8836817A77626D2071349ED248619,0,1600002618,2,1,1,496",
    "126D9AA3EBCFFAD92F0A0505FDC40AE9AFE918962A1D8923CB153ACD1EBA732,1,1600002642,3,0,1,332",
    "1A3D4E3559532FC64CEBBF45E78E42AE9454C8ADAB256A755B4F59CAAD23419,0,0,0,0,0,0",
    "1CF4F4E2D2172AF2C5B5483158C230D16E207A5CA1CCF6BABAEBA140D062262F,0,1600002583,3,0,1,588",
    "20183696B042288AD04635742E365BD5A487BFB1605599F8ECDC39CC51FF47,1,1600001697,3,3,1,443",
    "224BF134C6CDEA319E9CB05E4ACA9AD9A979E497A6FBC79BAA0F25F8F6BFA5B,0,1600001934,3,3,1,451",
    "28645ED9AB4399584190C5476EFFDCC616A09459CBE7BBB63B639D064EEBA22,0,0,0,0,0,0",
  },
  {
    "id_,test_flag,opportunity_timestamp,num_impressions,num_clicks,opportunity,total_spend",
    "0149BE3A4AB6B424CBDB47DB81F9544E9B3FDE7187399585819B589F9611E38,0,1600001404,2,0,1,300",
    "2499912C418FA70D3CA87BA47322977494B49E023B43C537C5E1D705DCF7A1,1,1600001587,1,1,1,111",
    "2AFC8D27B59FBB5C74BBE7B2D46B659CD89438198B89EA178C987C616776251F,0,0,0,0,0,0",
    "2D97E35F26B6A234CAC415CBC2E13138CCC3513A108958679E2B8658FF927F,1,1600002534,1,1,1,322",
    "32B1722F75F3681FE8E4BD7DC283A7F95BB32443D5A3CFFA28177D9EFB5418,0,0,0,0,0,0",
    "34E56397B276A699CFBC5BE45331E26A243DAC9A3C78EAC3B837E6436D9E927,0,1600002365,1,0,1,666",
    "3A61C0F39C972A7766941D59282240A74168CE9FCF61F288CEF9642F4E89650,1,1600001758,3,3,1,114",
    "3CA35EA9FA1F1852CAA776CDEBDF8B0EF43922333AAD183FEF3632F37146131,0,0,0,0,0,0",
    "4E4EF28D8819F3585E28527577484CE124FA24E085522DE7A868CA4E0977A,1,1600002059,2,2,1,462",
  }};
// clang-format on

TEST(ShardTest, RunWithOutputFilenames) {
  auto rand =
      folly::Random::secureRand64() % std::numeric_limits<int32_t>::max();
  std::string inputPath =
      "/tmp/ShardTest_RunWithOutputFilenames_in" + std::to_string(rand);
  data_processing::test_utils::writeVecToFile(inputLines, inputPath);

  std::string outputBasePath = "/tmp/ShardTest_RunWithOutputFilenames_out_";
  std::vector<std::string> outputFilenames{
      outputBasePath + std::to_string(rand),
      outputBasePath + std::to_string(rand + 1),
  };

  auto outputFilenamesStr = folly::join(',', outputFilenames);
  runShard(inputPath, outputFilenamesStr, "", 0, 2, 1'000'000);
  data_processing::test_utils::expectFileRowsEqual(
      outputFilenames.at(0), expectedOutBasic.at(0));
  data_processing::test_utils::expectFileRowsEqual(
      outputFilenames.at(1), expectedOutBasic.at(1));
}

TEST(ShardTest, RunWithOutputBasePath) {
  auto rand =
      folly::Random::secureRand64() % std::numeric_limits<int32_t>::max();
  std::string inputPath =
      "/tmp/ShardTest_RunWithOutputBasePath_in" + std::to_string(rand);
  data_processing::test_utils::writeVecToFile(inputLines, inputPath);

  std::string outputBasePath = "/tmp/ShardTest_RunWithOutputBasePath_out";
  std::vector<std::string> outputFilenames{
      outputBasePath + '_' + std::to_string(rand),
      outputBasePath + '_' + std::to_string(rand + 1)};

  runShard(
      inputPath, "", outputBasePath, static_cast<int32_t>(rand), 2, 1'000'000);
  data_processing::test_utils::expectFileRowsEqual(
      outputFilenames.at(0), expectedOutBasic.at(0));
  data_processing::test_utils::expectFileRowsEqual(
      outputFilenames.at(1), expectedOutBasic.at(1));
}

TEST(ShardTest, RunWithNoOutputFatal) {
  ASSERT_DEATH(runShard("/test/input", "", "", 0, 0, 0), "Error");
}

TEST(ShardPidTest, RunWithOutputFilenames) {
  auto rand =
      folly::Random::secureRand64() % std::numeric_limits<int32_t>::max();
  std::string inputPath =
      "/tmp/ShardPidTest_RunWithOutputFilenames_in" + std::to_string(rand);
  data_processing::test_utils::writeVecToFile(inputLines, inputPath);

  std::string outputBasePath = "/tmp/ShardPidTest_RunWithOutputFilenames_out_";
  std::vector<std::string> outputFilenames{
      outputBasePath + std::to_string(rand),
      outputBasePath + std::to_string(rand + 1),
  };

  auto outputFilenamesStr = folly::join(',', outputFilenames);
  runShardPid(inputPath, outputFilenamesStr, "", 0, 2, 1'000'000, "");
  data_processing::test_utils::expectFileRowsEqual(
      outputFilenames.at(0), expectedOutPid.at(0));
  data_processing::test_utils::expectFileRowsEqual(
      outputFilenames.at(1), expectedOutPid.at(1));
}

TEST(ShardPidTest, RunWithOutputBasePath) {
  auto rand =
      folly::Random::secureRand64() % std::numeric_limits<int32_t>::max();
  std::string inputPath =
      "/tmp/ShardPidTest_RunWithOutputBasePath_in" + std::to_string(rand);
  data_processing::test_utils::writeVecToFile(inputLines, inputPath);

  std::string outputBasePath = "/tmp/ShardPidTest_RunWithOutputBasePath_out";
  std::vector<std::string> outputFilenames{
      outputBasePath + '_' + std::to_string(rand),
      outputBasePath + '_' + std::to_string(rand + 1)};

  runShardPid(
      inputPath,
      "",
      outputBasePath,
      static_cast<int32_t>(rand),
      2,
      1'000'000,
      "");
  data_processing::test_utils::expectFileRowsEqual(
      outputFilenames.at(0), expectedOutPid.at(0));
  data_processing::test_utils::expectFileRowsEqual(
      outputFilenames.at(1), expectedOutPid.at(1));
}

TEST(ShardPidTest, RunWithNoOutputFatal) {
  ASSERT_DEATH(runShardPid("/test/input", "", "", 0, 0, 0, ""), "Error");
}
