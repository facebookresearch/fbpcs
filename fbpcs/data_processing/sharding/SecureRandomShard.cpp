/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <gflags/gflags.h>
#include <signal.h>

#include <fbpcf/aws/AwsSdk.h>
#include <folly/init/Init.h>

#include "fbpcf/engine/communication/SocketPartyCommunicationAgentFactory.h"
#include "fbpcs/data_processing/sharding/Sharding.h"

DEFINE_string(input_filename, "", "Name of the input file");
DEFINE_string(
    output_filenames,
    "",
    "Comma-separated list of file paths for output");
DEFINE_string(
    output_base_path,
    "",
    "Local or s3 base path where output files are written to");
DEFINE_int32(
    file_start_index,
    0,
    "First file that will be created from base path");
DEFINE_uint32(num_output_files, 0, "Number of files that should be created");
DEFINE_int32(log_every_n, 1000000, "How frequently to log updates");

DEFINE_int32(party, 1, "1 = publisher, 2 = partner");
DEFINE_string(server_ip, "127.0.0.1", "Server's IP address");
DEFINE_int32(port, 5000, "Server's port");
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

int main(int argc, char** argv) {
  folly::init(&argc, &argv);
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  fbpcf::AwsSdk::aquire();

  signal(SIGPIPE, SIG_IGN);

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
      std::make_shared<fbpcf::util::MetricCollector>("secure_random_shuffle");

  auto communicationAgentFactory = std::make_unique<
      fbpcf::engine::communication::SocketPartyCommunicationAgentFactory>(
      FLAGS_party - 1, partyInfos, tlsInfo, metricCollector);

  data_processing::sharder::runSecureRandomShard(
      FLAGS_input_filename,
      FLAGS_output_filenames,
      FLAGS_output_base_path,
      FLAGS_file_start_index,
      FLAGS_num_output_files,
      FLAGS_log_every_n,
      FLAGS_party == 1,
      communicationAgentFactory->create(
          2 - FLAGS_party, "secure_random_shuffle_traffic"));
  return 0;
}
