/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <glog/logging.h>
#include <filesystem>
#include <sstream>
#include <string>

#include "folly/init/Init.h"
#include "folly/logging/xlog.h"

#include "fbpcf/aws/AwsSdk.h"
#include "fbpcs/emp_games/lift/pcf2_calculator/MainUtil.h"
#include "fbpcs/performance_tools/CostEstimation.h"

DEFINE_int32(party, 1, "1 = publisher, 2 = partner");
DEFINE_string(server_ip, "127.0.0.1", "Server's IP Address");
DEFINE_int32(
    port,
    10000,
    "Network port for establishing connection to other player");
DEFINE_string(
    input_directory,
    "",
    "Data directory where input files are located");
DEFINE_string(
    input_filenames,
    "in.csv_0[,in.csv_1,in.csv_2,...]",
    "List of input file names that should be parsed (should have a header)");
DEFINE_string(
    output_directory,
    "",
    "Local or s3 path where output files are written to");
DEFINE_string(
    output_filenames,
    "out.csv_0[,out.csv_1,out.csv_2,...]",
    "List of output file names that correspond to input filenames (positionally)");
DEFINE_string(
    input_base_path,
    "",
    "Local or s3 base path for the sharded input files");
DEFINE_string(
    output_base_path,
    "",
    "Local or s3 base path where output files are written to");
DEFINE_int32(
    file_start_index,
    0,
    "First file that will be read with base path");
DEFINE_int32(num_files, 0, "Number of files that should be read");
DEFINE_int64(
    epoch,
    1546300800,
    "Unixtime of 2019-01-01. Used as our 'new epoch' for timestamps");
DEFINE_bool(
    is_conversion_lift,
    true,
    "Use conversion_lift logic (as opposed to converter_lift logic)");
DEFINE_bool(
    use_xor_encryption,
    true,
    "Reveal output with XOR secret shares instead of in the clear to both parties");
DEFINE_int32(
    num_conversions_per_user,
    25,
    "Cap and pad to this many conversions per user");
DEFINE_int32(
    concurrency,
    1,
    "max number of game(s) that will run concurrently?");
DEFINE_string(
    run_name,
    "",
    "A user given run name that will be used in s3 filename");
DEFINE_bool(
    log_cost,
    false,
    "Log cost info into cloud which will be used for dashboard");

int main(int argc, char** argv) {
  fbpcs::performance_tools::CostEstimation cost =
      fbpcs::performance_tools::CostEstimation("lift", "pcf2");
  cost.start();

  folly::init(&argc, &argv);
  fbpcf::AwsSdk::aquire();
  gflags::ParseCommandLineFlags(&argc, &argv, true);

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
               << "\tinput: " << inputFileLogList.str()
               << "\toutput: " << outputFileLogList.str();
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
            outputFilepaths,
            concurrency,
            FLAGS_server_ip,
            FLAGS_port,
            FLAGS_num_conversions_per_user,
            FLAGS_epoch,
            FLAGS_use_xor_encryption);
  } else if (FLAGS_party == common::PARTNER) {
    XLOG(INFO)
        << "Starting Private Lift as Partner, will wait for Publisher...";
    schedulerStatistics =
        private_lift::startCalculatorAppsForShardedFiles<common::PARTNER>(
            inputFilepaths,
            outputFilepaths,
            concurrency,
            FLAGS_server_ip,
            FLAGS_port,
            FLAGS_num_conversions_per_user,
            FLAGS_epoch,
            FLAGS_use_xor_encryption);
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
