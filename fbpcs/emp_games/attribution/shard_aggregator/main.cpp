/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <filesystem>
#include <stdexcept>

#include <gflags/gflags.h>

#include "folly/init/Init.h"
#include "folly/logging/xlog.h"

#include <fbpcf/aws/AwsSdk.h>
#include <fbpcf/exception/ExceptionBase.h>
#include <fbpcs/performance_tools/CostEstimation.h>
#include "MainUtil.h"
#include "ShardAggregatorApp.h"

DEFINE_int32(party, 1, "1 = publisher, 2 = partner");
DEFINE_int32(visibility, 0, "0 = public, 1 = publisher, 2 = partner");
DEFINE_string(server_ip, "", "Server's IP address");
DEFINE_int32(port, 15200, "Server's port");
DEFINE_string(input_base_path, "", "Input path where input files are located");
DEFINE_int32(
    first_shard_index,
    0,
    "index of first shard in input_path, first filename input_path_[first_shard_index]");
DEFINE_int32(
    num_shards,
    1,
    "Number of shards from input_path_[0] to input_path_[n-1]");
DEFINE_string(output_path, "", "Output path where output file is located");
DEFINE_int64(threshold, 100, "Threshold for K-anonymity");
DEFINE_string(
    metrics_format_type,
    "ad_object",
    "Options are 'ad_object' or 'lift'");
DEFINE_string(run_name, "", "User given name used to write cost info in S3");

int main(int argc, char* argv[]) {
  fbpcs::performance_tools::CostEstimation cost{"shard_aggregator"};
  cost.start();

  folly::init(&argc, &argv);
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  fbpcf::AwsSdk::aquire();

  XLOGF(INFO, "Party: {}", FLAGS_party);
  XLOGF(INFO, "Visibility: {}", FLAGS_visibility);
  XLOGF(INFO, "Server IP: {}", FLAGS_server_ip);
  XLOGF(INFO, "Port: {}", FLAGS_port);
  XLOGF(INFO, "Input path: {}", FLAGS_input_base_path);
  XLOGF(INFO, "Number of shards: {}", FLAGS_num_shards);
  XLOGF(INFO, "Output path: {}", FLAGS_output_path);
  XLOGF(INFO, "K-anonymity threshold: {}", FLAGS_threshold);

  XLOG(INFO) << "Start aggregating...";

  auto party = static_cast<fbpcf::Party>(FLAGS_party);
  auto visibility = static_cast<fbpcf::Visibility>(FLAGS_visibility);

  try {
    measurement::private_attribution::ShardAggregatorApp(
        party,
        visibility,
        FLAGS_server_ip,
        FLAGS_port,
        FLAGS_first_shard_index,
        FLAGS_num_shards,
        FLAGS_threshold,
        FLAGS_input_base_path,
        FLAGS_output_path,
        FLAGS_metrics_format_type)
        .run();
  } catch (const fbpcf::ExceptionBase& e) {
    XLOGF(ERR, "Some error occurred: {}", e.what());
    return 1;
  } catch (const std::exception& e) {
    XLOGF(ERR, "Some unknown error occurred: {}", e.what());
    return -1;
  }

  XLOGF(
      INFO,
      "Aggregation is completed. Please find the metrics at {}",
      FLAGS_output_path);

  cost.end();
  XLOG(INFO) << cost.getEstimatedCostString();
  if (FLAGS_run_name != "") {
    std::string runName = folly::to<std::string>(
        cost.getApplication(),
        "_",
        FLAGS_run_name,
        "_",
        measurement::private_attribution::getDateString());
    XLOG(INFO) << cost.writeToS3(
        runName, cost.getEstimatedCostDynamic(runName));
  }

  return 0;
}
