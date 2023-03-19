/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <folly/dynamic.h>
#include <gflags/gflags.h>
#include <glog/logging.h>
#include <signal.h>
#include <filesystem>
#include <string>
#include "fbpcf/aws/AwsSdk.h"
#include "fbpcf/engine/communication/SocketPartyCommunicationAgentFactory.h"
#include "fbpcs/emp_games/common/FeatureFlagUtil.h"
#include "fbpcs/performance_tools/CostEstimation.h"
#include "folly/String.h"
#include "folly/init/Init.h"
#include "folly/logging/xlog.h"

#include "fbpcf/mpc_std_lib/unified_data_process/data_processor/UdpEncryption.h"
#include "fbpcs/emp_games/data_processing/unified_data_process/UdpEncryptor/UdpEncryptorApp.h"

DEFINE_int32(party, 1, "1 = publisher, 2 = partner");
DEFINE_string(server_ip, "127.0.0.1", "Server's IP address");
DEFINE_int32(port, 5000, "Server's port");

DEFINE_string(
    data_base_path,
    "",
    "Local or s3 base path where serialized input data can be found.");
DEFINE_int32(data_num, 1, "number of input files");

DEFINE_string(
    index_base_path,
    "",
    "Local or s3 base path where indexes files can be found.");
DEFINE_int32(index_num, 1, "number of index files");

DEFINE_string(
    encryption_output_base_path,
    "",
    "Local or s3 base path to files to write encryption results");
DEFINE_int32(encryption_output_num, 1, "number of encryption files");

DEFINE_string(
    global_parameters_file,
    "",
    "Local or s3 base path to file storing global parameters.");

DEFINE_string(
    expanded_key_file,
    "",
    "Local or s3 base path where to write expanded key file.");

DEFINE_int32(
    chunk_size,
    50000,
    "the batch size for processing UDP encryption.");

DEFINE_bool(
    use_tls,
    false,
    "Whether to use TLS when communicating with other parties.");
DEFINE_string(
    ca_cert_path,
    "",
    "Relative file path where root CA cert is stored. It will be prefixed with $HOME.");
DEFINE_string(
    server_cert_path,
    "",
    "Relative file path where server cert is stored. It will be prefixed with $HOME.");
DEFINE_string(
    private_key_path,
    "",
    "Relative file path where private key is stored. It will be prefixed with $HOME.");

DEFINE_bool(
    log_cost,
    false,
    "Log cost info into cloud which will be used for dashboard");
DEFINE_string(log_cost_s3_bucket, "", "s3 bucket name");
DEFINE_string(
    log_cost_s3_region,
    ".s3.us-west-2.amazonaws.com/",
    "s3 regioni name");
DEFINE_string(
    run_name,
    "",
    "A user given run name that will be used in s3 filename");

std::vector<std::string> generateFileNames(
    const std::string& basename,
    int count) {
  std::vector<std::string> rst;
  for (size_t i = 0; i < count; i++) {
    rst.push_back(basename + "_" + std::to_string(i));
  }
  return rst;
}

int main(int argc, char* argv[]) {
  folly::init(&argc, &argv);
  gflags::ParseCommandLineFlags(&argc, &argv, true);

  fbpcs::performance_tools::CostEstimation cost =
      fbpcs::performance_tools::CostEstimation(
          "udp", FLAGS_log_cost_s3_bucket, FLAGS_log_cost_s3_region, "pcf2");

  cost.start();

  fbpcf::AwsSdk::aquire();

  signal(SIGPIPE, SIG_IGN);

  XLOGF(INFO, "Party: {}", FLAGS_party);
  XLOGF(INFO, "Server IP: {}", FLAGS_server_ip);
  XLOGF(INFO, "Port: {}", FLAGS_port);

  XLOGF(INFO, "Data path: {}", FLAGS_data_base_path);
  XLOGF(INFO, "index path: {}", FLAGS_index_base_path);
  XLOGF(INFO, "Global parameter path: {}", FLAGS_global_parameters_file);

  FLAGS_party--; // subtract 1 because we use 0 and 1 for publisher and partner
                 // instead of 1 and 2

  auto tlsInfo = fbpcf::engine::communication::getTlsInfoFromArgs(
      FLAGS_use_tls,
      FLAGS_ca_cert_path,
      FLAGS_server_cert_path,
      FLAGS_private_key_path,
      "");

  std::map<
      int,
      fbpcf::engine::communication::SocketPartyCommunicationAgentFactory::
          PartyInfo>
      partyInfos(
          {{0, {FLAGS_server_ip, FLAGS_port}},
           {1, {FLAGS_server_ip, FLAGS_port}}});

  auto metricCollector =
      std::make_shared<fbpcf::util::MetricCollector>("Udp_encryption_metrics");

  auto communicationAgentFactory = std::make_unique<
      fbpcf::engine::communication::SocketPartyCommunicationAgentFactory>(
      FLAGS_party, partyInfos, tlsInfo, metricCollector);

  unified_data_process::UdpEncryptorApp encryptionApp(
      std::make_unique<unified_data_process::UdpEncryptor>(
          std::make_unique<fbpcf::mpc_std_lib::unified_data_process::
                               data_processor::UdpEncryption>(
              communicationAgentFactory->create(
                  1 - FLAGS_party, "udp_encryption_traffic")),
          FLAGS_chunk_size),
      FLAGS_party == 0);

  encryptionApp.invokeUdpEncryption(
      generateFileNames(FLAGS_index_base_path, FLAGS_index_num),
      generateFileNames(FLAGS_data_base_path, FLAGS_data_num),
      FLAGS_global_parameters_file,
      generateFileNames(
          FLAGS_encryption_output_base_path, FLAGS_encryption_output_num),
      FLAGS_expanded_key_file);

  cost.end();
  XLOG(INFO, cost.getEstimatedCostString());

  if (FLAGS_log_cost) {
    bool run_name_specified = FLAGS_run_name != "";
    auto run_name = run_name_specified ? FLAGS_run_name : "temp_run_name";
    auto party = (FLAGS_party == 0) ? "Publisher" : "Partner";
    folly::dynamic costDict = cost.getEstimatedCostDynamic(
        run_name, party, metricCollector->collectMetrics());
    auto objectName = run_name_specified
        ? run_name
        : folly::to<std::string>(
              FLAGS_run_name, '_', costDict["timestamp"].asString());
    XLOGF(INFO, "{}", cost.writeToS3(party, objectName, costDict));
  }
  return 0;
}
