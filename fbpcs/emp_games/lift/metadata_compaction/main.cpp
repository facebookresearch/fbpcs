/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <sstream>
#include "folly/init/Init.h"
#include "folly/logging/xlog.h"

#include "fbpcf/aws/AwsSdk.h"
#include "fbpcs/performance_tools/CostEstimation.h"

#include "fbpcs/emp_games/common/Constants.h"
#include "fbpcs/emp_games/common/Util.h"
#include "fbpcs/emp_games/lift/metadata_compaction/MainUtil.h"
#include "fbpcs/emp_games/lift/metadata_compaction/MetadataCompactionOptions.h"

int main(int argc, char** argv) {
  folly::init(&argc, &argv);
  gflags::ParseCommandLineFlags(&argc, &argv, true);

  fbpcs::performance_tools::CostEstimation cost =
      fbpcs::performance_tools::CostEstimation(
          "lift_metadata_compaction",
          FLAGS_log_cost_s3_bucket,
          FLAGS_log_cost_s3_region,
          "pcf2");
  cost.start();

  fbpcf::AwsSdk::aquire();

  // since DEFINE_INT16 is not supported, cast int32_t FLAGS_concurrency to
  // int16_t is necessary here
  int16_t concurrency = static_cast<int16_t>(FLAGS_concurrency);
  CHECK_LE(concurrency, private_lift::kMaxConcurrency)
      << "Concurrency must be at most " << private_lift::kMaxConcurrency;

  auto filepaths = private_lift::getIOFilepaths(
      FLAGS_input_path,
      FLAGS_output_global_params_path,
      FLAGS_output_secret_shares_path,
      FLAGS_input_base_path,
      FLAGS_output_global_params_base_path,
      FLAGS_output_secret_shares_base_path,
      FLAGS_num_files,
      FLAGS_file_start_index);

  std::ostringstream inputFileLogList;
  for (auto filepath : filepaths.inputFilePaths) {
    inputFileLogList << "\t\t" << filepath << "\n";
  }

  std::ostringstream outputGlobalParamsFileLogList;
  for (auto filepath : filepaths.outputGlobalParamsFilePaths) {
    outputGlobalParamsFileLogList << "\t\t" << filepath << "\n";
  }

  std::ostringstream outputSecretSharesFileLogList;
  for (auto filepath : filepaths.outputSecretSharesFilePaths) {
    outputSecretSharesFileLogList << "\t\t" << filepath << "\n";
  }

  XLOG(INFO) << "Running lift metadata compaction with settings:\n"
             << "\tparty: " << FLAGS_party << "\n"
             << "\tuse_xor_encryption: " << FLAGS_use_xor_encryption << "\n"
             << "\tserver_ip_address: " << FLAGS_server_ip << "\n"
             << "\tport: " << FLAGS_port << "\n"
             << "\tinput: " << inputFileLogList.str() << "\n"
             << "\tglobal params output: "
             << outputGlobalParamsFileLogList.str() << "\n"
             << "\tsecret shares output: "
             << outputSecretSharesFileLogList.str() << "\n"
             << "\tepoch: " << FLAGS_epoch << "\n"
             << "\tnumber of conversions per user: "
             << FLAGS_num_conversions_per_user << "\n"
             << "\tcompute publisher breakdowns: "
             << FLAGS_compute_publisher_breakdowns << "\n"
             << "\trun_name: " << FLAGS_run_name << "\n"
             << "\tlog cost: " << FLAGS_log_cost << "\n"
             << "\ts3 bucket: " << FLAGS_log_cost_s3_bucket << "\n"
             << "\ts3 region: " << FLAGS_log_cost_s3_region << "\n"
             << "\tpc_feature_flags:" << FLAGS_pc_feature_flags;

  FLAGS_party--; // subtract 1 because we use 0 and 1 for publisher and partner
  // instead of 1 and 2

  auto tlsInfo = fbpcf::engine::communication::getTlsInfoFromArgs(
      FLAGS_use_tls,
      FLAGS_ca_cert_path,
      FLAGS_server_cert_path,
      FLAGS_private_key_path,
      "");

  common::SchedulerStatistics schedulerStatistics;

  XLOG(INFO) << "Start Metadata Compaction...";
  if (FLAGS_party == common::PUBLISHER) {
    XLOG(INFO)
        << "Starting Metadata Compaction as Publisher, will wait for Partner...";
    schedulerStatistics =
        private_lift::startMetadataCompactionApp<common::PUBLISHER>(
            filepaths.inputFilePaths,
            filepaths.outputGlobalParamsFilePaths,
            filepaths.outputSecretSharesFilePaths,
            FLAGS_concurrency,
            FLAGS_server_ip,
            FLAGS_port,
            FLAGS_num_conversions_per_user,
            FLAGS_compute_publisher_breakdowns,
            FLAGS_epoch,
            FLAGS_use_xor_encryption,
            tlsInfo);
  } else if (FLAGS_party == common::PARTNER) {
    XLOG(INFO)
        << "Starting Metadata Compaction as Partner, will wait for Publisher...";
    schedulerStatistics =
        private_lift::startMetadataCompactionApp<common::PARTNER>(
            filepaths.inputFilePaths,
            filepaths.outputGlobalParamsFilePaths,
            filepaths.outputSecretSharesFilePaths,
            FLAGS_concurrency,
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
        FLAGS_input_path,
        "",
        1,
        0,
        1,
        FLAGS_use_xor_encryption,
        schedulerStatistics);

    extra_info["output_secret_shares_path"] = FLAGS_output_secret_shares_path;
    extra_info["output_global_params_path"] = FLAGS_output_global_params_path;

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
