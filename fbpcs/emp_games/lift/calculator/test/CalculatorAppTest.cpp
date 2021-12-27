/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <filesystem>
#include <string>
#include <unordered_map>

#include <gtest/gtest.h>

#include "folly/Format.h"
#include "folly/Random.h"

#include <fbpcf/io/FileManagerUtil.h>
#include <fbpcf/mpc/EmpGame.h>
#include "../../../common/Csv.h"
#include "../CalculatorApp.h"
#include "common/GenFakeData.h"
#include "common/LiftCalculator.h"

constexpr int32_t tsOffset = 10;

DEFINE_bool(is_conversion_lift, true, "is conversion lift");
DEFINE_int32(num_conversions_per_user, 4, "num of conversions per user");
DEFINE_int64(epoch, 1546300800, "epoch");

namespace private_lift {
class CalculatorAppTest : public ::testing::Test {
 protected:
  void SetUp() override {
    port_ = 5000 + folly::Random::rand32() % 1000;
    std::string tempDir = std::filesystem::temp_directory_path();
    inputPathAlice_ = folly::sformat(
        "{}/input_alice_{}.csv", tempDir, folly::Random::secureRand64());
    inputPathBob_ = folly::sformat(
        "{}/input_bob_{}.csv", tempDir, folly::Random::secureRand64());
    outputPathAlice_ = folly::sformat(
        "{}/res_alice_{}", tempDir, folly::Random::secureRand64());
    outputPathBob_ =
        folly::sformat("{}/res_bob_{}", tempDir, folly::Random::secureRand64());

    GenFakeData testDataGenerator;
    LiftFakeDataParams params;

    params.setNumRows(15)
        .setOpportunityRate(0.5)
        .setTestRate(0.5)
        .setPurchaseRate(0.5)
        .setIncrementalityRate(0.0)
        .setEpoch(1546300800);
    testDataGenerator.genFakePublisherInputFile(inputPathAlice_, params);
    params.setNumConversions(4).setOmitValuesColumn(false);
    testDataGenerator.genFakePartnerInputFile(inputPathBob_, params);
  }

  void TearDown() override {
    std::filesystem::remove(outputPathAlice_);
    std::filesystem::remove(outputPathBob_);
    std::filesystem::remove(inputPathAlice_);
    std::filesystem::remove(inputPathBob_);
  }

  static void runGame(
      const fbpcf::Party party,
      const std::string& serverIp,
      const uint16_t port,
      const std::filesystem::path& inputPath,
      const std::string& outputPath,
      const bool useXorEncryption) {
    CalculatorApp(
        party, serverIp, port, inputPath, outputPath, useXorEncryption)
        .run();
  }

 protected:
  uint16_t port_;
  std::string inputPathAlice_;
  std::string inputPathBob_;
  std::string outputPathAlice_;
  std::string outputPathBob_;
};

TEST_F(CalculatorAppTest, RandomInputTestVisibilityPublic) {
  auto futureAlice = std::async(
      runGame,
      fbpcf::Party::Alice,
      "",
      port_,
      inputPathAlice_,
      outputPathAlice_,
      false /* useXorEncryption */);
  auto futureBob = std::async(
      runGame,
      fbpcf::Party::Bob,
      "127.0.0.1",
      port_,
      inputPathBob_,
      outputPathBob_,
      false /* useXorEncryption */);

  futureAlice.wait();
  futureBob.wait();

  LiftCalculator liftCalculator;
  std::ifstream inFileAlice{inputPathAlice_};
  std::ifstream inFileBob{inputPathBob_};
  std::string linePublisher;
  std::string linePartner;
  getline(inFileAlice, linePublisher);
  getline(inFileBob, linePartner);
  auto headerPublisher =
      private_measurement::csv::splitByComma(linePublisher, false);
  auto headerPartner =
      private_measurement::csv::splitByComma(linePartner, false);
  auto colNameToIndex =
      liftCalculator.mapColToIndex(headerPublisher, headerPartner);
  OutputMetricsData computedResult =
      liftCalculator.compute(inFileAlice, inFileBob, colNameToIndex, tsOffset);
  GroupedLiftMetrics expectedRes;
  expectedRes.metrics = computedResult.toLiftMetrics();

  auto resAlice =
      GroupedLiftMetrics::fromJson(fbpcf::io::read(outputPathAlice_));
  auto resBob = GroupedLiftMetrics::fromJson(fbpcf::io::read(outputPathBob_));
  EXPECT_EQ(expectedRes, resAlice);
  EXPECT_EQ(expectedRes, resBob);
}
} // namespace private_lift
