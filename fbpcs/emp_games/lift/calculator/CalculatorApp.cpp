/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "fbpcs/emp_games/lift/calculator/CalculatorApp.h"

#include <vector>

#include <gflags/gflags.h>

#include <fbpcf/io/FileManagerUtil.h>
#include <fbpcf/mpc/EmpApp.h>
#include <fbpcf/mpc/EmpGame.h>
#include <folly/logging/xlog.h>

#include "fbpcs/emp_games/lift/calculator/CalculatorGame.h"
#include "fbpcs/emp_games/lift/calculator/CalculatorGameConfig.h"
#include "fbpcs/emp_games/lift/calculator/LiftInputData.h"

namespace private_lift {
void CalculatorApp::run() {
  try {
    CalculatorGameConfig config = getInputData();
    int32_t numValues = static_cast<int32_t>(config.inputData.size());
    XLOG(INFO) << "Have " << numValues << " values in inputData.";

    XLOG(INFO) << "connecting...";
    std::unique_ptr<emp::NetIO> io = std::make_unique<emp::NetIO>(
        party_ == fbpcf::Party::Alice ? nullptr : serverIp_.c_str(), port_);

    CalculatorGame game{std::move(io), party_, visibility_};
    auto output = game.perfPlay(config);
    XLOG(INFO) << "done calculating";

    putOutputData(output);
  } catch (const std::exception& e) {
    XLOGF(
        ERR,
        "Error: Exception caught in CalculatorApp run.\n \t error msg: {} \n \t input shard: {}.",
        e.what(),
        inputPath_.u8string());
    std::exit(1);
  }
};

CalculatorGameConfig CalculatorApp::getInputData() {
  // Converter Lift always only supports 1 conversion per user
  int32_t numConversionsPerUser =
      FLAGS_is_conversion_lift ? FLAGS_num_conversions_per_user : 1;

  XLOG(INFO) << "Parsing input";
  LiftInputData inputData{party_, inputPath_};
  CalculatorGameConfig config = {
    std::move(inputData), FLAGS_is_conversion_lift, numConversionsPerUser};
  return config;
}

void CalculatorApp::putOutputData(const std::string& output) {
  XLOG(INFO) << "putting out data...";
  fbpcf::io::write(outputPath_, output);
}
} // namespace private_lift
