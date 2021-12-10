/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <gflags/gflags.h>
#include <string>

#include <fbpcf/mpc/EmpGame.h>
#include "folly/Format.h"
#include "folly/init/Init.h"
#include "folly/logging/xlog.h"

#include <fbpcf/aws/AwsSdk.h>
#include <fbpcf/mpc/MpcAppExecutor.h>
#include <fbpcs/performance_tools/CostEstimation.h>

#include "fbpcs/emp_games/attribution/decoupled_aggregation/AggregationOptions.h"
#include "fbpcs/emp_games/attribution/decoupled_aggregation/MainUtil.h"

int main(int argc, char* argv[]) {
  fbpcs::performance_tools::CostEstimation cost =
      fbpcs::performance_tools::CostEstimation("computation_experimental");
  cost.start();

  folly::init(&argc, &argv);
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  fbpcf::AwsSdk::aquire();

  XLOGF(INFO, "Party: {}", FLAGS_party);
  XLOGF(INFO, "Server IP: {}", FLAGS_server_ip);
  XLOGF(INFO, "Port: {}", FLAGS_port);
  XLOGF(
      INFO, "Input secret share path: {}", FLAGS_input_base_path_secret_share);
  XLOGF(INFO, "Input clear text path: {}", FLAGS_input_base_path);
  XLOGF(INFO, "Output path: {}", FLAGS_output_base_path);
  try {
    XLOG(INFO) << "Start private aggregation...";

    // Private attribution will have a secret share output, which will be the
    // input for aggregation game Along with corresponding clearText files
    // containing fields that were not a part of attribution game.
    auto inputSecretShareFilePaths =
        aggregation::private_aggregation::getIOInputFilenames(
            FLAGS_num_files,
            FLAGS_input_base_path_secret_share,
            FLAGS_file_start_index,
            FLAGS_use_postfix);

    auto inputClearTextFilePaths =
        aggregation::private_aggregation::getIOInputFilenames(
            FLAGS_num_files,
            FLAGS_input_base_path,
            FLAGS_file_start_index,
            FLAGS_use_postfix);

    auto outputFilePaths =
        aggregation::private_aggregation::getIOInputFilenames(
            FLAGS_num_files,
            FLAGS_output_base_path,
            FLAGS_file_start_index,
            FLAGS_use_postfix);

    int16_t concurrency = static_cast<int16_t>(FLAGS_concurrency);

    if (FLAGS_party == static_cast<int>(fbpcf::Party::Alice)) {
      XLOGF(INFO, "Aggregation Format: {}", FLAGS_aggregators);

      XLOG(INFO)
          << "Starting private aggregation as Publisher, will wait for Partner...";
      if (FLAGS_use_xor_encryption) {
        aggregation::private_aggregation::
            startPrivateAggregationApp<emp::ALICE, fbpcf::Visibility::Xor>(
                inputSecretShareFilePaths,
                inputClearTextFilePaths,
                outputFilePaths,
                FLAGS_server_ip,
                FLAGS_port,
                FLAGS_aggregators,
                concurrency);
      } else {
        aggregation::private_aggregation::
            startPrivateAggregationApp<emp::ALICE, fbpcf::Visibility::Public>(
                inputSecretShareFilePaths,
                inputClearTextFilePaths,
                outputFilePaths,
                FLAGS_server_ip,
                FLAGS_port,
                FLAGS_aggregators,
                concurrency);
      }

    } else if (FLAGS_party == static_cast<int>(fbpcf::Party::Bob)) {
      XLOG(INFO)
          << "Starting private aggregation as Partner, will wait for Publisher...";
      if (FLAGS_use_xor_encryption) {
        aggregation::private_aggregation::
            startPrivateAggregationApp<emp::BOB, fbpcf::Visibility::Xor>(
                inputSecretShareFilePaths,
                inputClearTextFilePaths,
                outputFilePaths,
                FLAGS_server_ip,
                FLAGS_port,
                FLAGS_aggregators,
                concurrency);
      } else {
        aggregation::private_aggregation::
            startPrivateAggregationApp<emp::BOB, fbpcf::Visibility::Public>(
                inputSecretShareFilePaths,
                inputClearTextFilePaths,
                outputFilePaths,
                FLAGS_server_ip,
                FLAGS_port,
                FLAGS_aggregators,
                concurrency);
      }

    } else {
      XLOGF(FATAL, "Invalid Party: {}", FLAGS_party);
    }

    cost.end();
    XLOG(INFO, cost.getEstimatedCostString());

    if (FLAGS_run_name != "" &&
        FLAGS_party == static_cast<int>(fbpcf::Party::Alice)) {
      XLOGF(
          INFO,
          "{}",
          cost.writeToS3(
              FLAGS_run_name,
              cost.getEstimatedCostDynamic(
                  FLAGS_run_name, "", FLAGS_aggregators)));
    }

    return 0;
  } catch (const std::exception& e) {
    XLOG(ERR)
        << "Error: Exception caught in Private Aggregation run.\n \t error msg: "
        << e.what()
        << "\n \t input directory: " << FLAGS_input_base_path_secret_share;
    return 1;
  }
}
