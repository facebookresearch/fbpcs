/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <gflags/gflags.h>
#include <signal.h>

#include "folly/Format.h"
#include "folly/init/Init.h"
#include "folly/logging/xlog.h"

#include <fbpcf/aws/AwsSdk.h>
#include <fbpcs/performance_tools/CostEstimation.h>

#include "fbpcs/emp_games/pcf2_aggregation/AggregationApp.h"
#include "fbpcs/emp_games/pcf2_aggregation/AggregationOptions.h"
#include "fbpcs/emp_games/pcf2_aggregation/Constants.h"
#include "fbpcs/emp_games/pcf2_aggregation/MainUtil.h"

int main(int argc, char* argv[]) {
  folly::init(&argc, &argv);
  gflags::ParseCommandLineFlags(&argc, &argv, true);

  fbpcs::performance_tools::CostEstimation cost =
      fbpcs::performance_tools::CostEstimation(
          "aggregator",
          FLAGS_log_cost_s3_bucket,
          FLAGS_log_cost_s3_region,
          "pcf2");
  cost.start();

  fbpcf::AwsSdk::aquire();

  signal(SIGPIPE, SIG_IGN);

  FLAGS_party--; // subtract 1 because we use 0 and 1 for publisher and partner
                 // instead of 1 and 2

  XLOGF(INFO, "Party: {}", FLAGS_party);
  XLOGF(INFO, "Server IP: {}", FLAGS_server_ip);
  XLOGF(INFO, "Port: {}", FLAGS_port);
  XLOGF(
      INFO, "Input secret share path: {}", FLAGS_input_base_path_secret_share);
  XLOGF(INFO, "Input clear text path: {}", FLAGS_input_base_path);
  XLOGF(INFO, "Base output path: {}", FLAGS_output_base_path);

  common::SchedulerStatistics schedulerStatistics;

  try {
    XLOG(INFO) << "Start private aggregation...";

    auto inputSecretShareFilePaths = pcf2_aggregation::getIOInputFilenames(
        FLAGS_num_files,
        FLAGS_input_base_path_secret_share,
        FLAGS_file_start_index,
        FLAGS_use_postfix);

    auto inputClearTextFilePaths = pcf2_aggregation::getIOInputFilenames(
        FLAGS_num_files,
        FLAGS_input_base_path,
        FLAGS_file_start_index,
        FLAGS_use_postfix);

    auto outputFilePaths = pcf2_aggregation::getIOInputFilenames(
        FLAGS_num_files,
        FLAGS_output_base_path,
        FLAGS_file_start_index,
        FLAGS_use_postfix);

    int16_t concurrency = static_cast<int16_t>(FLAGS_concurrency);
    CHECK_LE(concurrency, pcf2_aggregation::kMaxConcurrency)
        << "Concurrency must be at most " << pcf2_aggregation::kMaxConcurrency;

    common::Visibility outputVisibility = FLAGS_use_xor_encryption
        ? common::Visibility::Xor
        : common::Visibility::Publisher;

    common::InputEncryption inputEncryption;
    if (FLAGS_input_encryption == 1) {
      inputEncryption = common::InputEncryption::PartnerXor;
    } else if (FLAGS_input_encryption == 2) {
      inputEncryption = common::InputEncryption::Xor;
    } else {
      inputEncryption = common::InputEncryption::Plaintext;
    }

    if (FLAGS_party == common::PUBLISHER) {
      XLOGF(INFO, "Aggregation Format: {}", FLAGS_aggregators);

      XLOG(INFO)
          << "Starting private aggregation as Publisher, will wait for Partner...";

      schedulerStatistics =
          pcf2_aggregation::startAggregationAppsForShardedFiles<
              common::PUBLISHER>(
              inputEncryption,
              outputVisibility,
              inputSecretShareFilePaths,
              inputClearTextFilePaths,
              outputFilePaths,
              concurrency,
              FLAGS_server_ip,
              FLAGS_port,
              FLAGS_aggregators);
    } else if (FLAGS_party == common::PARTNER) {
      XLOG(INFO)
          << "Starting private aggregation as Partner, will wait for Publisher...";
      schedulerStatistics =
          pcf2_aggregation::startAggregationAppsForShardedFiles<
              common::PARTNER>(
              inputEncryption,
              outputVisibility,
              inputSecretShareFilePaths,
              inputClearTextFilePaths,
              outputFilePaths,
              concurrency,
              FLAGS_server_ip,
              FLAGS_port,
              FLAGS_aggregators);

    } else {
      XLOGF(FATAL, "Invalid Party: {}", FLAGS_party);
    }
  } catch (const std::exception& e) {
    XLOG(ERR)
        << "Error: Exception caught in Private Aggregation run.\n \t error msg: "
        << e.what()
        << "\n \t input directory: " << FLAGS_input_base_path_secret_share;
    std::exit(1);
  }

  cost.end();
  XLOG(INFO, cost.getEstimatedCostString());

  XLOGF(
      INFO,
      "Non-free gate count = {}, Free gate count = {}",
      schedulerStatistics.nonFreeGates,
      schedulerStatistics.freeGates);

  XLOGF(
      INFO,
      "Sent network traffic = {}, Received network traffic = {}",
      schedulerStatistics.sentNetwork,
      schedulerStatistics.receivedNetwork);

  if (FLAGS_log_cost) {
    bool run_name_specified = FLAGS_run_name != "";
    auto run_name = run_name_specified ? FLAGS_run_name : "temp_run_name";

    std::string party =
        (FLAGS_party == common::PUBLISHER) ? "Publisher" : "Partner";

    folly::dynamic extra_info = common::getCostExtraInfo(
        party,
        FLAGS_input_base_path,
        FLAGS_output_base_path,
        FLAGS_num_files,
        FLAGS_file_start_index,
        FLAGS_concurrency,
        FLAGS_use_xor_encryption,
        schedulerStatistics);

    folly::dynamic costDict =
        cost.getEstimatedCostDynamic(run_name, party, extra_info);

    auto objectName = run_name_specified
        ? run_name
        : folly::to<std::string>(
              FLAGS_run_name, '_', costDict["timestamp"].asString());

    XLOGF(INFO, "{}", cost.writeToS3(party, objectName, costDict));
  }

  return 0;
}
