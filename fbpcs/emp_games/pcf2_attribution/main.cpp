/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <gflags/gflags.h>
#include <signal.h>
#include <string>

#include "folly/Format.h"
#include "folly/init/Init.h"
#include "folly/logging/xlog.h"

#include <fbpcf/aws/AwsSdk.h>
#include <fbpcs/performance_tools/CostEstimation.h>

#include "fbpcs/emp_games/pcf2_attribution/AttributionApp.h"
#include "fbpcs/emp_games/pcf2_attribution/AttributionOptions.h"
#include "fbpcs/emp_games/pcf2_attribution/Constants.h"
#include "fbpcs/emp_games/pcf2_attribution/MainUtil.h"

int main(int argc, char* argv[]) {
  fbpcs::performance_tools::CostEstimation cost =
      fbpcs::performance_tools::CostEstimation("attributor", "pcf2");
  cost.start();

  folly::init(&argc, &argv);
  gflags::ParseCommandLineFlags(&argc, &argv, true);

  signal(SIGPIPE, SIG_IGN);

  fbpcf::AwsSdk::aquire();

  FLAGS_party--; // subtract 1 because we use 0 and 1 for publisher and partner
                 // instead of 1 and 2

  XLOGF(INFO, "Party: {}", FLAGS_party);
  XLOGF(INFO, "Server IP: {}", FLAGS_server_ip);
  XLOGF(INFO, "Port: {}", FLAGS_port);
  XLOGF(INFO, "Base input path: {}", FLAGS_input_base_path);
  XLOGF(INFO, "Base output path: {}", FLAGS_output_base_path);

  common::SchedulerStatistics schedulerStatistics;

  // use batched attribution by default
  const bool usingBatch = true;

  try {
    auto [inputFilenames, outputFilenames] = pcf2_attribution::getIOFilenames(
        FLAGS_num_files,
        FLAGS_input_base_path,
        FLAGS_output_base_path,
        FLAGS_file_start_index,
        FLAGS_use_postfix);
    int16_t concurrency = static_cast<int16_t>(FLAGS_concurrency);
    CHECK_LE(concurrency, pcf2_attribution::kMaxConcurrency)
        << "Concurrency must be at most " << pcf2_attribution::kMaxConcurrency;

    if (FLAGS_party == common::PUBLISHER) {
      XLOGF(INFO, "Attribution Rules: {}", FLAGS_attribution_rules);

      XLOG(INFO)
          << "Starting attribution as Publisher, will wait for Partner...";

      if (FLAGS_input_encryption == 1) {
        schedulerStatistics =
            pcf2_attribution::startAttributionAppsForShardedFiles<
                common::PUBLISHER,
                usingBatch,
                common::InputEncryption::PartnerXor>(
                inputFilenames,
                outputFilenames,
                concurrency,
                FLAGS_server_ip,
                FLAGS_port,
                FLAGS_attribution_rules,
                FLAGS_use_new_output_format);
      } else if (FLAGS_input_encryption == 2) {
        schedulerStatistics =
            pcf2_attribution::startAttributionAppsForShardedFiles<
                common::PUBLISHER,
                usingBatch,
                common::InputEncryption::Xor>(
                inputFilenames,
                outputFilenames,
                concurrency,
                FLAGS_server_ip,
                FLAGS_port,
                FLAGS_attribution_rules,
                FLAGS_use_new_output_format);
      } else {
        schedulerStatistics =
            pcf2_attribution::startAttributionAppsForShardedFiles<
                common::PUBLISHER,
                usingBatch,
                common::InputEncryption::Plaintext>(
                inputFilenames,
                outputFilenames,
                concurrency,
                FLAGS_server_ip,
                FLAGS_port,
                FLAGS_attribution_rules,
                FLAGS_use_new_output_format);
      }

    } else if (FLAGS_party == common::PARTNER) {
      XLOG(INFO)
          << "Starting attribution as Partner, will wait for Publisher...";

      if (FLAGS_input_encryption == 1) {
        schedulerStatistics =
            pcf2_attribution::startAttributionAppsForShardedFiles<
                common::PARTNER,
                usingBatch,
                common::InputEncryption::PartnerXor>(
                inputFilenames,
                outputFilenames,
                concurrency,
                FLAGS_server_ip,
                FLAGS_port,
                FLAGS_attribution_rules,
                FLAGS_use_new_output_format);
      } else if (FLAGS_input_encryption == 2) {
        schedulerStatistics =
            pcf2_attribution::startAttributionAppsForShardedFiles<
                common::PARTNER,
                usingBatch,
                common::InputEncryption::Xor>(
                inputFilenames,
                outputFilenames,
                concurrency,
                FLAGS_server_ip,
                FLAGS_port,
                FLAGS_attribution_rules,
                FLAGS_use_new_output_format);

      } else {
        schedulerStatistics =
            pcf2_attribution::startAttributionAppsForShardedFiles<
                common::PARTNER,
                usingBatch,
                common::InputEncryption::Plaintext>(
                inputFilenames,
                outputFilenames,
                concurrency,
                FLAGS_server_ip,
                FLAGS_port,
                FLAGS_attribution_rules,
                FLAGS_use_new_output_format);
      }

    } else {
      XLOGF(FATAL, "Invalid Party: {}", FLAGS_party);
    }

  } catch (const std::exception& e) {
    XLOG(ERR) << "Error: Exception caught in Attribution run.\n \t error msg: "
              << e.what() << "\n \t input directory: " << FLAGS_input_base_path;
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
