/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <cstdint>
#include <filesystem>
#include <string>
#include <unordered_map>
#include <vector>

#include <gtest/gtest.h>

#include <fbpcf/mpc/EmpGame.h>
#include "folly/Format.h"
#include "folly/Random.h"
#include "folly/logging/xlog.h"

#include "fbpcs/emp_games/attribution/decoupled_aggregation/test/AggregationTestUtils.h"
#include "fbpcs/emp_games/common/TestUtil.h"

namespace aggregation::private_aggregation {

class AggregationAppTest : public ::testing::Test {
 protected:
  void SetUp() override {
    port_ = 5000 + folly::Random::rand32() % 1000;
    baseDir_ = private_measurement::test_util::getBaseDirFromPath(__FILE__);
    std::string tempDir = std::filesystem::temp_directory_path();
    serverIpAlice_ = "";
    serverIpBob_ = "127.0.0.1";
    outputPathAlice_ = folly::sformat(
        "{}/output_path_alice.json_{}", tempDir, folly::Random::secureRand64());
    outputPathBob_ = folly::sformat(
        "{}/output_path_bob.json_{}", tempDir, folly::Random::secureRand64());
  }

  void TearDown() override {
    std::filesystem::remove(outputPathAlice_);
    std::filesystem::remove(outputPathBob_);
  }

  std::string serverIpAlice_;
  std::string serverIpBob_;
  uint16_t port_;
  std::string baseDir_;
  std::string outputPathAlice_;
  std::string outputPathBob_;
};

// Test cases are iterate in https://fb.quip.com/IUHDApxKEAli
TEST_F(AggregationAppTest, TestMPCAEMCorrectness) {
  // Attribution rules we want to test - this are the attribution rules for
  // which attribution layer has already been run and we have the output results
  // from that layer.
  std::vector<std::string> attribution_rules{"last_click_1d", "last_touch_1d"};
  // Currently only one aggregation format - measurement.
  std::vector<std::string> aggregation_formats{"measurement"};

  for (auto attribution_rule : attribution_rules) {
    for (auto aggregation_format : aggregation_formats) {
      // modifiable input parameters
      std::string inputPrefix = "test_correctness";
      // inputPrefix should is sufficient in specifying the right input data
      // for both Alice (publisher) and bob (partner)
      std::string aggregationFormatAlice = aggregation_format;
      std::string aggregationFormatBob = "";

      std::string OutputJsonFileName = baseDir_ + inputPrefix + "/" +
          attribution_rule + "." + aggregationFormatAlice + ".json";

      auto [resAlice, resBob] = runGameAndGenOutput<fbpcf::Visibility::Public>(
          serverIpAlice_,
          port_,
          aggregationFormatAlice,
          baseDir_ + inputPrefix + "/" + attribution_rule + ".publisher.json",
          baseDir_ + inputPrefix + "/" + attribution_rule + ".publisher.csv",
          outputPathAlice_,
          serverIpBob_,
          port_,
          aggregationFormatBob,
          baseDir_ + inputPrefix + "/" + attribution_rule + ".partner.json",
          baseDir_ + inputPrefix + "/" + attribution_rule + ".partner.csv",
          outputPathBob_);

      // verify whether the output is what we expected
      verifyOutput(resAlice, resBob, OutputJsonFileName);
    }
  }
}

// Test cases are iterate in https://fb.quip.com/IUHDApxKEAli
TEST_F(AggregationAppTest, TestMPCAEMCorrectnessWithPrivateScaling) {
  // Attribution rules we want to test - this are the attribution rules for
  // which attribution layer has already been run and we have the output results
  // from that layer.
  std::vector<std::string> attribution_rules{"last_click_1d", "last_touch_1d"};
  // Currently only one aggregation format - measurement.
  std::vector<std::string> aggregation_formats{"measurement"};

  for (auto attribution_rule : attribution_rules) {
    for (auto aggregation_format : aggregation_formats) {
      // modifiable input parameters
      std::string inputPrefix = "test_correctness";
      // inputPrefix should is sufficient in specifying the right input data
      // for both Alice (publisher) and bob (partner)
      std::string aggregationFormatAlice = aggregation_format;
      std::string aggregationFormatBob = "";

      std::string OutputJsonFileName = baseDir_ + inputPrefix + "/" +
          attribution_rule + "." + aggregationFormatAlice + ".json";

      auto [resAlice, resBob] = runGameAndGenOutput<fbpcf::Visibility::Xor>(
          serverIpAlice_,
          port_,
          aggregationFormatAlice,
          baseDir_ + inputPrefix + "/" + attribution_rule + ".publisher.json",
          baseDir_ + inputPrefix + "/" + attribution_rule + ".publisher.csv",
          outputPathAlice_,
          serverIpBob_,
          port_,
          aggregationFormatBob,
          baseDir_ + inputPrefix + "/" + attribution_rule + ".partner.json",
          baseDir_ + inputPrefix + "/" + attribution_rule + ".partner.csv",
          outputPathBob_);

      // for XORed cases, additional step to decode real answer
      auto [revealedResAlice, revealedresBob] = revealXORedResult(
          resAlice, resBob, aggregation_format, attribution_rule);

      // verify whether the output is what we expected
      verifyOutput(revealedResAlice, revealedresBob, OutputJsonFileName);
    }
  }
}
} // namespace aggregation::private_aggregation
