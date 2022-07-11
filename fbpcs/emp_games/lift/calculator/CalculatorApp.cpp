/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <vector>

#include <gflags/gflags.h>
#include "folly/logging/xlog.h"

#include <fbpcf/io/api/FileIOWrappers.h>
#include <fbpcf/mpc/EmpApp.h>
#include <fbpcf/mpc/EmpGame.h>

#include "CalculatorApp.h"
#include "CalculatorGame.h"
#include "CalculatorGameConfig.h"
#include "InputData.h"

namespace private_lift {
void CalculatorApp::run() {
  try {
    CalculatorGameConfig config = getInputData();
    int32_t numValues = static_cast<int32_t>(config.inputData.getNumRows());
    XLOG(INFO) << "Have " << numValues << " values in inputData.";

    XLOG(INFO) << "connecting...";
    std::unique_ptr<emp::NetIO> io = std::make_unique<emp::NetIO>(
        party_ == fbpcf::Party::Alice ? nullptr : serverIp_.c_str(), port_);

    CalculatorGame game{std::move(io), party_, visibility_};
    auto output = game.perfPlay(config);
    XLOG(INFO) << "done calculating";

    putOutputData(output);
  } catch (const std::exception& e) {
    auto path = inputPath_.u8string();
    XLOGF(
        ERR,
        "Error: Exception caught in CalculatorApp run.\n \t error msg: {} \n \t input shard: {}.",
        e.what(),
        reinterpret_cast<const char*>(path.c_str()));
    std::exit(1);
  }
};

CalculatorGameConfig CalculatorApp::getInputData() {
  // Converter Lift always only supports 1 conversion per user
  int32_t numConversionsPerUser =
      FLAGS_is_conversion_lift ? FLAGS_num_conversions_per_user : 1;

  auto liftGranularityType = FLAGS_is_conversion_lift
      ? InputData::LiftGranularityType::Conversion
      : InputData::LiftGranularityType::Converter;

  XLOG(INFO) << "Parsing input";
  InputData inputData{
      inputPath_,
      InputData::LiftMPCType::Standard,
      liftGranularityType,
      FLAGS_epoch,
      numConversionsPerUser};
  CalculatorGameConfig config = {
      inputData, FLAGS_is_conversion_lift, numConversionsPerUser};
  return config;
}

void CalculatorApp::putOutputData(const std::string& output) {
  XLOG(INFO) << "putting out data...";
  fbpcf::io::FileIOWrappers::writeFile(outputPath_, output);
}
} // namespace private_lift
