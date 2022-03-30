/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <cstdlib>
#include <filesystem>
#include <string>

#include <glog/logging.h>

#include <fbpcf/mpc/EmpApp.h>
#include <fbpcf/mpc/EmpGame.h>
#include "CalculatorGame.h"
#include "InputData.h"
#include "OutputMetrics.h"

// so that these FLAGS set in main.cpp are visible here
DECLARE_bool(is_conversion_lift);
DECLARE_int32(num_conversions_per_user);
DECLARE_int64(epoch);

namespace private_lift {

class CalculatorApp : public fbpcf::EmpApp<
                          CalculatorGame<emp::NetIO>,
                          CalculatorGameConfig,
                          std::string> {
 public:
  CalculatorApp(
      const fbpcf::Party party,
      const std::string& serverIp,
      const uint16_t port,
      const std::filesystem::path& inputPath,
      const std::string& outputPath,
      const bool useXorEncryption)
      : fbpcf::EmpApp<
            CalculatorGame<emp::NetIO>,
            CalculatorGameConfig,
            std::string>{party, serverIp, port},
        inputPath_(inputPath),
        outputPath_(outputPath),
        visibility_(
            useXorEncryption ? fbpcf::Visibility::Xor
                             : fbpcf::Visibility::Public) {}

  CalculatorApp(
      const fbpcf::Party party,
      const std::string& serverIp,
      const uint16_t port,
      const std::filesystem::path& inputPath,
      const std::string& outputPath,
      const bool useXorEncryption,
      bool useTls,
      const std::string& tlsDir_)
      : fbpcf::EmpApp<
            CalculatorGame<emp::NetIO>,
            CalculatorGameConfig,
            std::string>{party, serverIp, port, useTls, tlsDir_},
        inputPath_(inputPath),
        outputPath_(outputPath),
        visibility_(
            useXorEncryption ? fbpcf::Visibility::Xor
                             : fbpcf::Visibility::Public) {}

  void run() override;

 protected:
  CalculatorGameConfig getInputData() override;
  void putOutputData(const std::string& output) override;

 private:
  std::filesystem::path inputPath_;
  std::string outputPath_;
  fbpcf::Visibility visibility_;
};

} // namespace private_lift
