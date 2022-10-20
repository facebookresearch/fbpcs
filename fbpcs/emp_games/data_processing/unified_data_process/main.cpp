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
#include "fbpcs/emp_games/common/Util.h"
#include "fbpcs/emp_games/data_processing/unified_data_process/MainUtil.h"
#include "fbpcs/emp_games/data_processing/unified_data_process/UdpProcessApp.h"
#include "fbpcs/emp_games/data_processing/unified_data_process/UdpProcessOptions.h"

int main(int argc, char** argv) {
  folly::init(&argc, &argv);
  gflags::ParseCommandLineFlags(&argc, &argv, true);

  fbpcs::performance_tools::CostEstimation cost =
      fbpcs::performance_tools::CostEstimation(
          "data_processing_udp",
          FLAGS_log_cost_s3_bucket,
          FLAGS_log_cost_s3_region,
          "pcf2");
  cost.start();

  fbpcf::AwsSdk::aquire();

  XLOG(INFO) << "Running UDP library with settings:\n"
             << "\tparty: " << FLAGS_party << "\n"
             << "\tuse_xor_encryption: " << FLAGS_use_xor_encryption << "\n"
             << "\tserver_ip_address: " << FLAGS_server_ip << "\n"
             << "\tport: " << FLAGS_port << "\n"
             << "\trow_number: " << FLAGS_row_number << "\n"
             << "\trow_size: " << FLAGS_row_size << "\n"
             << "\tintersection: " << FLAGS_intersection << "\n"
             << "\trun_name: " << FLAGS_run_name << "\n"
             << "\tlog cost: " << FLAGS_log_cost << "\n"
             << "\ts3 bucket: " << FLAGS_log_cost_s3_bucket << "\n"
             << "\ts3 region: " << FLAGS_log_cost_s3_region << "\n"
             << "\tpc_feature_flags:" << FLAGS_pc_feature_flags;

  FLAGS_party--; // subtract 1 because we use 0 and 1 for publisher and partner
  // instead of 1 and 2

  auto tlsInfo = common::getTlsInfoFromArgs(false, "", "", "", "");

  common::SchedulerStatistics schedulerStatistics;

  XLOG(INFO) << "Start UDP Processing...";
  if (FLAGS_party == common::PUBLISHER) {
    XLOG(INFO)
        << "Starting UDP Processing as Publisher, will wait for Partner...";
    schedulerStatistics =
        unified_data_process::startUdpProcessApp<common::PUBLISHER>(
            FLAGS_server_ip,
            FLAGS_port,
            FLAGS_row_number,
            FLAGS_row_size,
            FLAGS_intersection,
            FLAGS_use_xor_encryption,
            tlsInfo);
  } else if (FLAGS_party == common::PARTNER) {
    XLOG(INFO)
        << "Starting UDP Processing as Partner, will wait for Publisher...";
    schedulerStatistics =
        unified_data_process::startUdpProcessApp<common::PARTNER>(
            FLAGS_server_ip,
            FLAGS_port,
            FLAGS_row_number,
            FLAGS_row_size,
            FLAGS_intersection,
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

    folly::dynamic extra_info = schedulerStatistics.details;

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
