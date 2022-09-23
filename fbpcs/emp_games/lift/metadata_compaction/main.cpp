/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "folly/init/Init.h"
#include "folly/logging/xlog.h"

#include "fbpcf/aws/AwsSdk.h"
#include "fbpcs/performance_tools/CostEstimation.h"

#include "fbpcs/emp_games/common/Constants.h"
#include "fbpcs/emp_games/lift/metadata_compaction/MainUtil.h"
#include "fbpcs/emp_games/lift/metadata_compaction/MetadataCompactionOptions.h"

int main(int argc, char** argv) {
  folly::init(&argc, &argv);
  gflags::ParseCommandLineFlags(&argc, &argv, true);

  fbpcf::AwsSdk::aquire();

  XLOG(INFO) << "Running lift metadata compaction with settings:\n"
             << "\tparty: " << FLAGS_party << "\n"
             << "\tuse_xor_encryption: " << FLAGS_use_xor_encryption << "\n"
             << "\tserver_ip_address: " << FLAGS_server_ip << "\n"
             << "\tport: " << FLAGS_port << "\n"
             << "\tinput: " << FLAGS_input_path << "\n"
             << "\tglobal params output: " << FLAGS_output_global_params_path
             << "\n"
             << "\tsecret shares output: " << FLAGS_output_secret_shares_path
             << "\n"
             << "\tepoch: " << FLAGS_epoch << "\n"
             << "\tnumber of conversions per user: "
             << FLAGS_num_conversions_per_user << "\n"
             << "\tcompute publisher breakdowns: "
             << FLAGS_compute_publisher_breakdowns << "\n"
             << "\tpc_feature_flags:" << FLAGS_pc_feature_flags;

  FLAGS_party--; // subtract 1 because we use 0 and 1 for publisher and partner
  // instead of 1 and 2
  common::SchedulerStatistics schedulerStatistics;

  XLOG(INFO) << "Start Metadata Compaction...";
  if (FLAGS_party == common::PUBLISHER) {
    XLOG(INFO)
        << "Starting Metadata Compaction as Publisher, will wait for Partner...";
    schedulerStatistics =
        private_lift::startMetadataCompactionApp<common::PUBLISHER>(
            FLAGS_input_path,
            FLAGS_output_global_params_path,
            FLAGS_output_secret_shares_path,
            FLAGS_server_ip,
            FLAGS_port,
            FLAGS_num_conversions_per_user,
            FLAGS_compute_publisher_breakdowns,
            FLAGS_epoch,
            FLAGS_use_xor_encryption);
  } else if (FLAGS_party == common::PARTNER) {
    XLOG(INFO)
        << "Starting Metadata Compaction as Partner, will wait for Publisher...";
    schedulerStatistics =
        private_lift::startMetadataCompactionApp<common::PARTNER>(
            FLAGS_input_path,
            FLAGS_output_global_params_path,
            FLAGS_output_secret_shares_path,
            FLAGS_server_ip,
            FLAGS_port,
            FLAGS_num_conversions_per_user,
            FLAGS_compute_publisher_breakdowns,
            FLAGS_epoch,
            FLAGS_use_xor_encryption);
  } else {
    XLOGF(FATAL, "Invalid Party: {}", FLAGS_party);
  }

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

  return 0;
}
