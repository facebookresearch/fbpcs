/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <string>
#include <vector>

#include <fbpcf/io/FileManagerUtil.h>
#include <fbpcf/mpc/EmpApp.h>
#include <fbpcf/mpc/EmpGame.h>
#include "fbpcs/emp_games/attribution/decoupled_aggregation/AggregationGame.h"
#include "fbpcs/emp_games/attribution/decoupled_aggregation/AggregationMetrics.h"
#include "fbpcs/emp_games/common/SecretSharing.h"

namespace aggregation::private_aggregation {

template <int MY_ROLE, fbpcf::Visibility OUTPUT_VISIBILITY>
class AggregationApp
    : public fbpcf::EmpApp<
          AggregationGame<MY_ROLE, emp::NetIO, OUTPUT_VISIBILITY>,
          AggregationInputMetrics,
          AggregationOutputMetrics> {
 public:
  AggregationApp(
      const std::string& serverIp,
      const uint16_t& port,
      const std::string& aggregationFormat,
      const std::string& inputSecretShareFilePath,
      const std::string& inputClearTextFilePath,
      const std::string& outputPath)
      : fbpcf::EmpApp<
            AggregationGame<MY_ROLE, emp::NetIO, OUTPUT_VISIBILITY>,
            AggregationInputMetrics,
            AggregationOutputMetrics>{static_cast<fbpcf::Party>(MY_ROLE), serverIp, port},
        aggregationFormat_{aggregationFormat},
        inputSecretShareFilePath_{inputSecretShareFilePath},
        inputClearTextFilePath_{inputClearTextFilePath},
        outputPath_{outputPath} {}

 protected:
  AggregationInputMetrics getInputData() override {
    XLOG(INFO) << "MY_ROLE: " << MY_ROLE
               << ", aggregationFormat_: " << aggregationFormat_
               << ", input_secret_share_file_path_: "
               << inputSecretShareFilePath_
               << ", input_clear_text_file_path_: " << inputClearTextFilePath_;

    return AggregationInputMetrics{
        MY_ROLE,
        inputSecretShareFilePath_,
        inputClearTextFilePath_,
        aggregationFormat_};
  }

  void putOutputData(
      const AggregationOutputMetrics& aggregationOutput) override {
    fbpcf::io::write(outputPath_, aggregationOutput.toJson());
  }

 private:
  std::string aggregationFormat_;
  std::string inputSecretShareFilePath_;
  std::string inputClearTextFilePath_;
  std::string outputPath_;
  std::string aggregationFieldsConfigPath_;
};

} // namespace aggregation::private_aggregation
