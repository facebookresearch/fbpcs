/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <gflags/gflags.h>
#include <string>

#include "folly/Format.h"
#include "folly/init/Init.h"
#include "folly/logging/xlog.h"

#include <fbpcf/aws/AwsSdk.h>
#include <fbpcs/performance_tools/CostEstimation.h>

#include "fbpcs/emp_games/common/Constants.h"
#include "fbpcs/emp_games/dotproduct/DotproductApp.h"
#include "fbpcs/emp_games/dotproduct/DotproductOptions.h"
#include "fbpcs/emp_games/dotproduct/MainUtil.h"

int main(int argc, char* argv[]) {
  folly::init(&argc, &argv);
  gflags::ParseCommandLineFlags(&argc, &argv, true);

  fbpcs::performance_tools::CostEstimation cost =
      fbpcs::performance_tools::CostEstimation(
          "dotproduct",
          FLAGS_log_cost_s3_bucket,
          FLAGS_log_cost_s3_region,
          "pcf2");
  cost.start();

  fbpcf::AwsSdk::aquire();

  FLAGS_party--; // subtract 1 because we use 0 and 1 for publisher and partner
                 // instead of 1 and 2

  XLOGF(INFO, "Party: {}", FLAGS_party);
  XLOGF(INFO, "Server IP: {}", FLAGS_server_ip);
  XLOGF(INFO, "Port: {}", FLAGS_port);
  XLOGF(INFO, "Base input path: {}", FLAGS_input_base_path);
  XLOGF(INFO, "Base output path: {}", FLAGS_output_base_path);

  common::SchedulerStatistics schedulerStatistics;

  auto tlsInfo = common::getTlsInfoFromArgs(
      FLAGS_use_tls,
      FLAGS_ca_cert_path,
      FLAGS_server_cert_path,
      FLAGS_private_key_path,
      "");
  try {
    if (FLAGS_party == common::PUBLISHER) {
      XLOG(INFO)
          << "Starting Dotproduct as Publisher, will wait for Partner...";

      schedulerStatistics =
          pcf2_dotproduct::startDotProductApp<common::PUBLISHER>(
              FLAGS_server_ip,
              FLAGS_port,
              FLAGS_input_base_path,
              FLAGS_output_base_path,
              FLAGS_num_features,
              FLAGS_label_width,
              FLAGS_debug,
              tlsInfo);

    } else if (FLAGS_party == common::PARTNER) {
      XLOG(INFO)
          << "Starting Dotproduct as Partner, will wait for Publisher...";

      schedulerStatistics =
          pcf2_dotproduct::startDotProductApp<common::PARTNER>(
              FLAGS_server_ip,
              FLAGS_port,
              FLAGS_input_base_path,
              FLAGS_output_base_path,
              FLAGS_num_features,
              FLAGS_label_width,
              FLAGS_debug,
              tlsInfo);
    } else {
      XLOGF(FATAL, "Invalid Party: {}", FLAGS_party);
    }

  } catch (const std::exception& e) {
    XLOG(ERR) << "Error: Exception caught in Dotproduct run.\n \t error msg: "
              << e.what() << "\n \t input file: " << FLAGS_input_base_path;
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
    auto party = (FLAGS_party == common::PUBLISHER) ? "Publisher" : "Partner";

    folly::dynamic extra_info = folly::dynamic::object(
        "publisher_input_path", (FLAGS_party == common::PUBLISHER) ? FLAGS_input_base_path : "")
        ("partner_input_basepath", (FLAGS_party == common::PARTNER) ? FLAGS_input_base_path : "")
        ("publisher_output_basepath", (FLAGS_party == common::PUBLISHER) ? FLAGS_output_base_path : "")
        ("partner_output_basepath", (FLAGS_party ==  common::PARTNER) ? FLAGS_output_base_path : "")
        ("num_features", FLAGS_num_features)
        ("label_width", FLAGS_label_width)
        ("non_free_gates", schedulerStatistics.nonFreeGates)
        ("free_gates", schedulerStatistics.freeGates)
        ("scheduler_transmitted_network", schedulerStatistics.sentNetwork)
        ("scheduler_received_network", schedulerStatistics.receivedNetwork)
        ("mpc_traffic_details", schedulerStatistics.details);

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
