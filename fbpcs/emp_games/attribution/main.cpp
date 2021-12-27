/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <gflags/gflags.h>
#include <string>

#include <fbpcf/mpc/EmpGame.h>
#include "folly/init/Init.h"
#include "folly/logging/xlog.h"

#include <fbpcf/aws/AwsSdk.h>
#include <fbpcf/mpc/MpcAppExecutor.h>
#include <fbpcs/performance_tools/CostEstimation.h>
#include "AttributionApp.h"
#include "MainUtil.h"
#include "fbpcs/emp_games/attribution/AttributionOptions.h"

int main(int argc, char* argv[]) {
  fbpcs::performance_tools::CostEstimation cost =
      fbpcs::performance_tools::CostEstimation("attribution");
  cost.start();

  XLOG(INFO) << "Start of main, printing network stats: "
             << measurement::private_attribution::exec("cat /proc/net/dev");

  folly::init(&argc, &argv);
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  fbpcf::AwsSdk::aquire();

  OMNISCIENT_ONLY_XLOG(INFO, "~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~");
  OMNISCIENT_ONLY_XLOG(INFO, "~~~~~~~~~OMNISCIENT LOGGING ENABLED~~~~~~~~~");
  OMNISCIENT_ONLY_XLOG(INFO, "~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~");

  XLOGF(INFO, "Party: {}", FLAGS_party);
  XLOGF(INFO, "Server IP: {}", FLAGS_server_ip);
  XLOGF(INFO, "Port: {}", FLAGS_port);
  XLOGF(INFO, "Base input path: {}", FLAGS_input_base_path);
  XLOGF(INFO, "Base output path: {}", FLAGS_output_base_path);

  try {
    auto [inputFilenames, outputFilenames] =
        measurement::private_attribution::getIOFilenames(
            FLAGS_num_files,
            FLAGS_input_base_path,
            FLAGS_output_base_path,
            FLAGS_file_start_index);
    int16_t concurrency = static_cast<int16_t>(FLAGS_concurrency);

    // construct attributionApps according to the number of files and
    // FLAGS_concurrency
    if (FLAGS_party == static_cast<int>(fbpcf::Party::Alice)) {
      XLOGF(INFO, "Attribution Rules: {}", FLAGS_attribution_rules);
      XLOGF(INFO, "Aggregators: {}", FLAGS_aggregators);

      XLOG(INFO)
          << "Starting attribution as Publisher, will wait for Partner...";

      if (FLAGS_use_xor_encryption) {
        measurement::private_attribution::startAttributionAppsForShardedFiles<
            measurement::private_attribution::PUBLISHER,
            fbpcf::Visibility::Xor>(
            inputFilenames,
            outputFilenames,
            concurrency,
            FLAGS_server_ip,
            FLAGS_port,
            FLAGS_attribution_rules,
            FLAGS_aggregators);
      } else {
        measurement::private_attribution::startAttributionAppsForShardedFiles<
            measurement::private_attribution::PUBLISHER,
            fbpcf::Visibility::Public>(
            inputFilenames,
            outputFilenames,
            concurrency,
            FLAGS_server_ip,
            FLAGS_port,
            FLAGS_attribution_rules,
            FLAGS_aggregators);
      }

    } else if (FLAGS_party == static_cast<int>(fbpcf::Party::Bob)) {
      XLOG(INFO)
          << "Starting attribution as Partner, will wait for Publisher...";

      if (FLAGS_use_xor_encryption) {
        measurement::private_attribution::startAttributionAppsForShardedFiles<
            measurement::private_attribution::PARTNER,
            fbpcf::Visibility::Xor>(
            inputFilenames,
            outputFilenames,
            concurrency,
            FLAGS_server_ip,
            FLAGS_port,
            FLAGS_attribution_rules,
            FLAGS_aggregators);
      } else {
        measurement::private_attribution::startAttributionAppsForShardedFiles<
            measurement::private_attribution::PARTNER,
            fbpcf::Visibility::Public>(
            inputFilenames,
            outputFilenames,
            concurrency,
            FLAGS_server_ip,
            FLAGS_port,
            FLAGS_attribution_rules,
            FLAGS_aggregators);
      }
    } else {
      XLOGF(FATAL, "Invalid Party: {}", FLAGS_party);
    }

    XLOG(INFO) << "*********************";
    XLOGF(
        INFO,
        "Attribution is completed. Please find the metrics at {}",
        FLAGS_output_base_path);
  } catch (const std::exception& e) {
    XLOG(ERR) << "Error: Exception caught in Attribution run.\n \t error msg: "
              << e.what() << "\n \t input directory: " << FLAGS_input_base_path;
    XLOG(INFO) << "End of main, printing network stats: "
               << measurement::private_attribution::exec("cat /proc/net/dev");
    std::exit(1);
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
                FLAGS_run_name, FLAGS_attribution_rules, FLAGS_aggregators)));
  }

  // TODO: remove the following and use cost.getNetworkBytes() or
  // cost.getEstimatedCost() instead
  XLOG(INFO) << "End of main, printing network stats: "
             << measurement::private_attribution::exec("cat /proc/net/dev");

  return 0;
}
