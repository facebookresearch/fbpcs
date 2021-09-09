/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <sstream>
#include <string>
#include <vector>

#include <emp-sh2pc/emp-sh2pc.h>
#include <folly/init/Init.h>
#include <gflags/gflags.h>
#include <glog/logging.h>

#include "InputData.h"
#include "OutputMetricsCalculator.h"

DEFINE_int32(role, 1, "1 = publisher, 2 = partner");
DEFINE_string(server_ip, "127.0.0.1", "Server's IP Address");
DEFINE_int32(port, 5000, "Network port for connecting to other player");
DEFINE_string(input_filepath, "", "Filepath to this player's input");
DEFINE_string(output_filepath, "", "Filepath where results should be output");
DEFINE_bool(
    use_xor_encryption,
    false,
    "Reveal output with XOR secret shares instead of clear to both parties");

using namespace measurement::private_reach;

template <int MY_ROLE>
void runCohortReachCircuit(
    const std::string& address,
    uint16_t port,
    bool useXOREncryption,
    InputData& inputData) {
  int32_t numValues = static_cast<int32_t>(inputData.getNumRows());
  LOG(INFO) << "Have " << numValues << " values in inputData.";

  LOG(INFO) << "connecting...";
  std::unique_ptr<emp::NetIO> io = std::make_unique<emp::NetIO>(
      MY_ROLE == PUBLISHER ? nullptr : address.c_str(), port);
  setup_semi_honest(io.get(), MY_ROLE);

  OutputMetricsCalculator<MY_ROLE> calculator{inputData, useXOREncryption};
  calculator.calculateAll();
  auto subOut = calculator.getCohortMetrics();

  // Print each cohort header. Note that the publisher won't know anything about
  // the cohort header (only a generic index cor which group we are outputting).
  for (auto i = 0; i < subOut.size(); ++i) {
    LOG(INFO) << "\nCohort [" << i << "] results:";
    if constexpr (MY_ROLE == PARTNER) {
      auto features = inputData.getCohortIdToFeatures().at(i);
      std::stringstream headerStream;
      for (auto j = 0; j < features.size(); ++j) {
        auto featureHeader = inputData.getFeatureHeader().at(j);
        headerStream << featureHeader << "=" << features.at(j);
        if (j + 1 < features.size()) {
          headerStream << ", ";
        }
      }
      LOG(INFO) << headerStream.str();
    } else {
      LOG(INFO) << "(Feature header unknown to publisher)";
    }

    auto cohortMetrics = subOut.at(i);
    LOG(INFO) << cohortMetrics;
  }
}

int main(int argc, char** argv) {
  folly::init(&argc, &argv);
  gflags::ParseCommandLineFlags(&argc, &argv, true);

  LOG(INFO) << "Running cohort reach with settings:\n"
            << "role: " << FLAGS_role << "\n"
            << "server_ip_address: " << FLAGS_server_ip << "\n"
            << "port: " << FLAGS_port << "\n"
            << "useXOREncryption: " << FLAGS_use_xor_encryption << "\n"
            << "inputFile: " << FLAGS_input_filepath << "\n";
  InputData inputData{FLAGS_input_filepath};

  if (FLAGS_role == 1) {
    runCohortReachCircuit<PUBLISHER>(
        FLAGS_server_ip, FLAGS_port, FLAGS_use_xor_encryption, inputData);
  } else {
    runCohortReachCircuit<PARTNER>(
        FLAGS_server_ip, FLAGS_port, FLAGS_use_xor_encryption, inputData);
  }

  return 0;
}
