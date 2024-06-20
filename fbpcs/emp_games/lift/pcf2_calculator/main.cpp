/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <gflags/gflags.h>
#include <glog/logging.h>
#include <signal.h>
#include <algorithm>
#include <filesystem>
#include <sstream>
#include <string>

#include "folly/String.h"
#include "folly/init/Init.h"
#include "folly/logging/xlog.h"

#include "fbpcf/aws/AwsSdk.h"
#include "fbpcs/emp_games/common/FeatureFlagUtil.h"
#include "fbpcs/emp_games/lift/pcf2_calculator/LiftOptions.h"
#include "fbpcs/emp_games/lift/pcf2_calculator/MainUtil.h"
#include "fbpcs/performance_tools/CostEstimation.h"

int main(int argc, char** argv) {
  folly::init(&argc, &argv);
  gflags::ParseCommandLineFlags(&argc, &argv, true);

  fbpcs::performance_tools::CostEstimation cost =
      fbpcs::performance_tools::CostEstimation(
          "lift", FLAGS_log_cost_s3_bucket, FLAGS_log_cost_s3_region, "pcf2");
  cost.start();

  fbpcf::AwsSdk::aquire();

  signal(SIGPIPE, SIG_IGN);

  // since DEFINE_INT16 is not supported, cast int32_t FLAGS_concurrency to
  // int16_t is necessary here
  int16_t concurrency = static_cast<int16_t>(FLAGS_concurrency);
  CHECK_LE(concurrency, private_lift::kMaxConcurrency)
      << "Concurrency must be at most " << private_lift::kMaxConcurrency;

  auto filepaths = private_lift::getIOFilepaths(
      FLAGS_input_base_path,
      FLAGS_output_base_path,
      FLAGS_input_directory,
      FLAGS_output_directory,
      FLAGS_input_filenames,
      FLAGS_output_filenames,
      FLAGS_num_files,
      FLAGS_file_start_index);
  auto inputFilepaths = filepaths.first;
  auto outputFilepaths = filepaths.second;

  auto tlsInfo = fbpcf::engine::communication::getTlsInfoFromArgs(
      FLAGS_use_tls,
      FLAGS_ca_cert_path,
      FLAGS_server_cert_path,
      FLAGS_private_key_path,
      "");

  bool readInputFromSecretShares = private_measurement::isFeatureFlagEnabled(
      FLAGS_pc_feature_flags, "private_lift_unified_data_process");

  bool useDecoupledUDP = private_measurement::isFeatureFlagEnabled(
      FLAGS_pc_feature_flags, "pcs_private_lift_decoupled_udp");

  {
    // Build a quick list of input/output files to log
    std::ostringstream inputFileLogList;
    for (auto inputFilepath : inputFilepaths) {
      inputFileLogList << "\t\t" << inputFilepath << "\n";
    }
    std::ostringstream outputFileLogList;
    for (auto outputFilepath : outputFilepaths) {
      outputFileLogList << "\t\t" << outputFilepath << "\n";
    }
    XLOG(INFO) << "Running conversion lift with settings:\n"
               << "\tparty: " << FLAGS_party << "\n"
               << "\tserver_ip_address: " << FLAGS_server_ip << "\n"
               << "\tport: " << FLAGS_port << "\n"
               << "\tconcurrency: " << FLAGS_concurrency << "\n"
               << "\tnumber of conversions per user: "
               << FLAGS_num_conversions_per_user << "\n"
               << "\tpc_feature_flags:" << FLAGS_pc_feature_flags
               << "\tinput: " << inputFileLogList.str()
               << "\toutput: " << outputFileLogList.str() << "\n"
               << "\tread from secret share: " << readInputFromSecretShares
               << "\tuse decoupled udp: " << useDecoupledUDP
               << "\tinput expanded key path: " << FLAGS_input_expanded_key_path
               << "\tinput global params path: "
               << FLAGS_input_global_params_path << "\n"
               << "\trun_id: " << FLAGS_run_id;
  }

  FLAGS_party--; // subtract 1 because we use 0 and 1 for publisher and partner
                 // instead of 1 and 2
  common::SchedulerStatistics schedulerStatistics;

  XLOG(INFO) << "Start Private Lift...";
  if (FLAGS_party == common::PUBLISHER) {
    XLOG(INFO)
        << "Starting Private Lift as Publisher, will wait for Partner...";
    schedulerStatistics =
        private_lift::startCalculatorAppsForShardedFiles<common::PUBLISHER>(
            inputFilepaths,
            FLAGS_input_global_params_path,
            FLAGS_input_expanded_key_path,
            outputFilepaths,
            readInputFromSecretShares,
            useDecoupledUDP,
            concurrency,
            FLAGS_server_ip,
            FLAGS_port,
            FLAGS_num_conversions_per_user,
            FLAGS_compute_publisher_breakdowns,
            FLAGS_epoch,
            FLAGS_use_xor_encryption,
            tlsInfo);
  } else if (FLAGS_party == common::PARTNER) {
    XLOG(INFO)
        << "Starting Private Lift as Partner, will wait for Publisher...";
    schedulerStatistics =
        private_lift::startCalculatorAppsForShardedFiles<common::PARTNER>(
            inputFilepaths,
            FLAGS_input_global_params_path,
            FLAGS_input_expanded_key_path,
            outputFilepaths,
            readInputFromSecretShares,
            useDecoupledUDP,
            concurrency,
            FLAGS_server_ip,
            FLAGS_port,
            FLAGS_num_conversions_per_user,
            FLAGS_compute_publisher_breakdowns,
            FLAGS_epoch,
            FLAGS_use_xor_encryption,
            tlsInfo);
  } else {
    XLOGF(FATAL, "Invalid Party: {}", FLAGS_party);
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
    auto party = (FLAGS_party == common::PUBLISHER) ? "Publisher" : "Partner";

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
