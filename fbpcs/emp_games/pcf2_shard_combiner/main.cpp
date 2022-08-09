/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <signal.h>

#include <filesystem>
#include <stdexcept>

#include <gflags/gflags.h>

#include <folly/Synchronized.h>
#include <folly/init/Init.h>
#include <folly/logging/xlog.h>

#include <fbpcf/aws/AwsSdk.h>
#include <fbpcf/exception/ExceptionBase.h>
#include <fbpcf/exception/exceptions.h>

#include "fbpcs/emp_games/common/Constants.h"
#include "fbpcs/emp_games/common/SchedulerStatistics.h"
#include "fbpcs/emp_games/pcf2_shard_combiner/AggMetrics.h"
#include "fbpcs/emp_games/pcf2_shard_combiner/AggMetrics_impl.h"
#include "fbpcs/emp_games/pcf2_shard_combiner/ShardValidator.h"
#include "fbpcs/emp_games/pcf2_shard_combiner/util/MainUtil.h"
#include "fbpcs/performance_tools/CostEstimation.h"

DEFINE_int32(party, 1, "1 = publisher, 2 = partner");
DEFINE_int32(
    visibility,
    0,
    "0 = public, 1 = publisher, 2 = partner"); // keeping this for consistency,
                                               // TODO: fix order with
                                               // ResultVisibility enum.
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
DEFINE_bool(
    log_cost,
    false,
    "Log cost info into cloud which will be used for dashboard");
DEFINE_string(log_cost_s3_bucket, "cost-estimation-logs", "s3 bucket name");
DEFINE_string(
    log_cost_s3_region,
    ".s3.us-west-2.amazonaws.com/",
    "s3 regioni name");

DEFINE_bool(
    useTls,
    false,
    "flag to enable tls, note: if true, you have to provide tlsDir option.");
DEFINE_string(tlsDir, "", "Directory to find tls certs.");
DEFINE_bool(
    use_xor_encryption,
    true,
    "Use XOR encryption while communicating intermediate results(uses LazyScheduler).");

using namespace shard_combiner;

int main(int argc, char* argv[]) {
  folly::init(&argc, &argv);
  gflags::ParseCommandLineFlags(&argc, &argv, true);

  fbpcs::performance_tools::CostEstimation cost{
      "shard_combiner", FLAGS_log_cost_s3_bucket, FLAGS_log_cost_s3_region};
  cost.start();

  fbpcf::AwsSdk::aquire();
  // Ignore broken pipe signal, so that we finish the application incase ssh
  // connection breaks.
  signal(SIGPIPE, SIG_IGN);

  XLOGF(INFO, "Party: {}", FLAGS_party);
  XLOGF(INFO, "Visibility: {}", FLAGS_visibility);
  XLOGF(INFO, "Server IP: {}", FLAGS_server_ip);
  XLOGF(INFO, "Port: {}", FLAGS_port);
  XLOGF(INFO, "Input path: {}", FLAGS_input_base_path);
  XLOGF(INFO, "Number of shards: {}", FLAGS_num_shards);
  XLOGF(INFO, "Output path: {}", FLAGS_output_path);
  XLOGF(INFO, "K-anonymity threshold: {}", FLAGS_threshold);

  // we use scheduler thats either 0 or 1,
  FLAGS_party--;
  assert(FLAGS_party == 0 || FLAGS_party == 1);

  bool usingBatch = false;
  common::InputEncryption inputEncryption = common::InputEncryption::Xor;

  std::string inputPath =
      FLAGS_input_base_path.substr(0, FLAGS_input_base_path.rfind("/"));
  std::string inputFilePrefix = FLAGS_input_base_path.substr(
      FLAGS_input_base_path.rfind("/") + 1, std::string::npos);

  common::SchedulerStatistics schedulerStatistics;

  if (FLAGS_metrics_format_type == "ad_object") {
    schedulerStatistics = runApp<ShardSchemaType::kAdObjFormat>(
        FLAGS_party,
        usingBatch,
        inputEncryption,
        FLAGS_num_shards,
        FLAGS_first_shard_index,
        inputPath,
        inputFilePrefix,
        FLAGS_output_path,
        FLAGS_threshold,
        FLAGS_useTls,
        FLAGS_tlsDir,
        FLAGS_use_xor_encryption,
        FLAGS_visibility,
        FLAGS_server_ip,
        FLAGS_port);
  } else if (FLAGS_metrics_format_type == "lift") {
    schedulerStatistics = runApp<ShardSchemaType::kGroupedLiftMetrics>(
        FLAGS_party,
        usingBatch,
        inputEncryption,
        FLAGS_num_shards,
        FLAGS_first_shard_index,
        inputPath,
        inputFilePrefix,
        FLAGS_output_path,
        FLAGS_threshold,
        FLAGS_useTls,
        FLAGS_tlsDir,
        FLAGS_use_xor_encryption,
        FLAGS_visibility,
        FLAGS_server_ip,
        FLAGS_port);
  } else {
    std::string errStr = folly::sformat(
        "unsupported metrics format type: {}", FLAGS_metrics_format_type);
    XLOG(ERR) << errStr;
    throw common::exceptions::NotSupportedError(errStr);
  }

  cost.end();
  XLOG(INFO) << cost.getEstimatedCostString();
  if (FLAGS_log_cost) {
    std::string party_str =
        (FLAGS_party == static_cast<int32_t>(common::PUBLISHER)) ? "Publisher"
                                                                 : "Partner";
    folly::dynamic extra_info = folly::dynamic::object(
      "publisher_input_basepath",
      party_str == "Publisher" ? FLAGS_input_base_path : "")
      ("partner_input_basepath",
      party_str == "Partner" ? FLAGS_input_base_path : "")
      ("output_path", FLAGS_output_path)
      ("num_shards", FLAGS_num_shards)
      ("first_shard_index", FLAGS_first_shard_index)
      ("metrics_format_type", FLAGS_metrics_format_type)
      ("threshold", FLAGS_threshold)
      ("use_xor_encryption", FLAGS_use_xor_encryption)
      ("non_free_gates", schedulerStatistics.nonFreeGates)
      ("free_gates", schedulerStatistics.freeGates)
      ("scheduler_transmitted_network", schedulerStatistics.sentNetwork)
      ("scheduler_received_network", schedulerStatistics.receivedNetwork)
      ("mpc_traffic_details", schedulerStatistics.details);

    folly::dynamic costDict =
        cost.getEstimatedCostDynamic(FLAGS_run_name, party_str, extra_info);

    auto objectName = folly::to<std::string>(
        FLAGS_run_name, '_', costDict["timestamp"].asString());

    std::string costWriteStatus =
        cost.writeToS3(party_str, objectName, costDict);
    XLOGF(INFO, "{}", costWriteStatus);
  }
  return 0;
}
