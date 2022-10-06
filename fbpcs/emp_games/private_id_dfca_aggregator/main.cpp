/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <fbpcf/aws/AwsSdk.h>
#include <fbpcf/engine/communication/SocketPartyCommunicationAgentFactory.h>
#include <fbpcf/io/api/FileIOWrappers.h>
#include <fbpcf/scheduler/LazySchedulerFactory.h>
#include <folly/init/Init.h>
#include <folly/logging/xlog.h>
#include <glog/logging.h>
#include <signal.h>
#include <filesystem>
#include <fstream>
#include <sstream>
#include <string>

#include "fbpcs/emp_games/common/Constants.h"
#include "fbpcs/emp_games/common/Util.h"
#include "fbpcs/emp_games/private_id_dfca_aggregator/PrivateIdDfcaAggregatorApp.h"
#include "fbpcs/emp_games/private_id_dfca_aggregator/PrivateIdDfcaAggregatorOptions.h"
#include "fbpcs/performance_tools/CostEstimation.h"

int main(int argc, char* argv[]) {
  folly::init(&argc, &argv);
  gflags::ParseCommandLineFlags(&argc, &argv, true);

  fbpcs::performance_tools::CostEstimation cost{
      "private_id_dfca_aggregator",
      FLAGS_log_cost_s3_bucket,
      FLAGS_log_cost_s3_region};
  cost.start();

  fbpcf::AwsSdk::aquire();
  signal(SIGPIPE, SIG_IGN);

  XLOGF(INFO, "Party: {}", FLAGS_party);
  XLOGF(INFO, "Server IP: {}", FLAGS_server_ip);
  XLOGF(INFO, "Port: {}", FLAGS_port);
  XLOGF(INFO, "Input path: {}", FLAGS_input_base_path);
  XLOGF(INFO, "Number of shards: {}", FLAGS_num_shards);
  XLOGF(INFO, "Output path: {}", FLAGS_output_path);

  FLAGS_party--;
  assert(FLAGS_party == 0 || FLAGS_party == 1);

  std::string inputPath =
      FLAGS_input_base_path.substr(0, FLAGS_input_base_path.rfind("/"));
  std::string inputFilePrefix = FLAGS_input_base_path.substr(
      FLAGS_input_base_path.rfind("/") + 1, std::string::npos);

  std::map<
      int,
      fbpcf::engine::communication::SocketPartyCommunicationAgentFactory::
          PartyInfo>
      partyInfos{
          {{common::PUBLISHER, {FLAGS_server_ip, FLAGS_port}},
           {common::PARTNER, {FLAGS_server_ip, FLAGS_port}}}};

  auto tlsInfo = common::getTlsInfoFromArgs(
      FLAGS_use_tls,
      FLAGS_ca_cert_path,
      FLAGS_server_cert_path,
      FLAGS_private_key_path,
      "");

  auto commAgentFactory = std::make_unique<
      fbpcf::engine::communication::SocketPartyCommunicationAgentFactory>(
      FLAGS_party,
      std::move(partyInfos),
      tlsInfo,
      "private_id_dfca_aggregator_traffic");

  auto app = private_id_dfca_aggregator::PrivateIdDfcaAggregatorApp(
      std::move(commAgentFactory));

  app.run(
      FLAGS_party,
      FLAGS_num_shards,
      FLAGS_first_shard_index,
      inputPath,
      inputFilePrefix,
      FLAGS_output_path);

  cost.end();
  XLOG(INFO) << cost.getEstimatedCostString();
  if (FLAGS_log_cost) {
    std::string party_str =
        (FLAGS_party == static_cast<int32_t>(common::PUBLISHER)) ? "Publisher"
                                                                 : "Partner";
    folly::dynamic extra_info = folly::dynamic::object(
        "publisher_input_basepath",
        party_str == "Publisher" ? FLAGS_input_base_path : "")(
        "partner_input_basepath",
        party_str == "Partner" ? FLAGS_input_base_path : "")(
        "output_path",
        FLAGS_output_path)("num_shards", FLAGS_num_shards)("first_shard_index", FLAGS_first_shard_index);

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
