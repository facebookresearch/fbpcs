/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <glog/logging.h>
#include <signal.h>
#include <filesystem>
#include <fstream>
#include <sstream>
#include <string>

#include "folly/init/Init.h"
#include "folly/logging/xlog.h"

#include "fbpcf/engine/communication/SocketPartyCommunicationAgentFactory.h"
#include "fbpcs/emp_games/compactor/AttributionOutput.h"
#include "fbpcs/emp_games/compactor/CompactorGame.h"

constexpr int32_t PUBLISHER_ROLE = 0;
constexpr int32_t PARTNER_ROLE = 1;

const int8_t adIdWidth = 64;
const int8_t convWidth = 32;

using AttributionValue = std::pair<
    fbpcf::mpc_std_lib::util::Intp<false, adIdWidth>,
    fbpcf::mpc_std_lib::util::Intp<false, convWidth>>;

DEFINE_int32(party, 0, "0 = publisher, 1 = partner");
DEFINE_string(host, "127.0.0.1", "Server's IP Address");
DEFINE_int32(
    port,
    8080,
    "Network port for establishing connection to other player");
DEFINE_string(input_file_path, "", "Local or s3 base path for input files");
DEFINE_string(
    output_file_path,
    "",
    "Local or s3 base path where output files are written to");

int main(int argc, char** argv) {
  folly::init(&argc, &argv);
  gflags::ParseCommandLineFlags(&argc, &argv, true);

  auto input = compactor::readXORShareInput(FLAGS_input_file_path);

  XLOG(INFO) << "Creating communication agent factory\n";
  std::map<
      int,
      fbpcf::engine::communication::SocketPartyCommunicationAgentFactory::
          PartyInfo>
      partyInfos{
          {{PUBLISHER_ROLE, {FLAGS_host, FLAGS_port}},
           {PARTNER_ROLE, {FLAGS_host, FLAGS_port}}}};
  auto commAgentFactory = std::make_unique<
      fbpcf::engine::communication::SocketPartyCommunicationAgentFactory>(
      FLAGS_party, std::move(partyInfos), "compactor_traffic");

  XLOG(INFO) << "Creating scheduler\n";
  auto scheduler = fbpcf::scheduler::createLazySchedulerWithRealEngine(
      FLAGS_party, *commAgentFactory);

  XLOG(INFO) << "Starting game\n";
  auto game = compactor::ShuffleBasedCompactorGame<AttributionValue, 0>(
      std::move(scheduler), FLAGS_party, 1 - FLAGS_party);
  compactor::SecretAttributionOutput<0> secret(input);
  auto rst = game.play(secret, input.size(), true);

  XLOG(INFO) << "Game done!\n";

  auto rstAd = rst.adId.extractIntShare().getValue();
  auto rstConv = rst.conversionValue.extractIntShare().getValue();
  auto rstLabel = rst.isAttributed.extractBit().getValue();

  // write output into file
  std::ofstream outputFile(FLAGS_output_file_path);
  outputFile << "AdId,conversionValue,isAttributed\n";
  for (size_t i = 0; i < rstAd.size(); i++) {
    outputFile << rstAd.at(i) << "," << rstConv.at(i) << "," << rstLabel.at(i)
               << "\n";
  }
  outputFile.close();

  XLOG(INFO) << "output size:" << rstAd.size() << std::endl;

  auto gateStats = fbpcf::scheduler::SchedulerKeeper<0>::getGateStatistics();
  XLOG(INFO) << "Non-free gate count: " << gateStats.first << '\n';
  XLOG(INFO) << "Free gate count: " << gateStats.second << '\n';

  auto trafficStats =
      fbpcf::scheduler::SchedulerKeeper<0>::getTrafficStatistics();
  XLOG(INFO) << "Tx bytes: " << trafficStats.first << '\n';
  XLOG(INFO) << "Rx bytes: " << trafficStats.second << '\n';

  return 0;
}
