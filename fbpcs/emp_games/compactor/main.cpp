/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <folly/init/Init.h>
#include <folly/logging/xlog.h>
#include <glog/logging.h>
#include <signal.h>
#include <filesystem>
#include <fstream>
#include <sstream>
#include <string>

#include "fbpcf/aws/AwsSdk.h"
#include "fbpcf/engine/communication/SocketPartyCommunicationAgentFactory.h"
#include "fbpcf/io/api/FileIOWrappers.h"
#include "fbpcs/emp_games/compactor/AttributionOutput.h"
#include "fbpcs/emp_games/compactor/CompactorGame.h"
#include "fbpcs/performance_tools/CostEstimation.h"

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
DEFINE_string(
    run_name,
    "",
    "A user given run name that will be used in s3 filename");
DEFINE_bool(
    log_cost,
    false,
    "Log cost info into cloud which will be used for dashboard");
DEFINE_string(log_cost_s3_bucket, "cost-estimation-logs", "s3 bucket name");
DEFINE_string(
    log_cost_s3_region,
    ".s3.us-west-2.amazonaws.com/",
    "s3 region name");
int main(int argc, char** argv) {
  folly::init(&argc, &argv);
  gflags::ParseCommandLineFlags(&argc, &argv, true);

  fbpcs::performance_tools::CostEstimation cost =
      fbpcs::performance_tools::CostEstimation(
          "compactor",
          FLAGS_log_cost_s3_bucket,
          FLAGS_log_cost_s3_region,
          "pcf2");
  cost.start();

  XLOG(INFO) << "Party:" << FLAGS_party << "\n";
  XLOG(INFO) << "Host:" << FLAGS_host << "\n";
  XLOG(INFO) << "port:" << FLAGS_port << "\n";
  XLOG(INFO) << "Input file:" << FLAGS_input_file_path << "\n";
  XLOG(INFO) << "Output file:" << FLAGS_output_file_path << "\n";
  XLOG(INFO) << " Log cost:" << FLAGS_log_cost << "\n";

  fbpcf::AwsSdk::aquire();

  XLOG(INFO) << "Reading input file: " << FLAGS_input_file_path << "\n";
  auto input = compactor::readXORShareInput(FLAGS_input_file_path);
  XLOG(INFO) << "Finished reading " << FLAGS_input_file_path
             << ", size: " << input.size() << "\n";

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
  std::stringstream content;
  content << "adId,conversionValue,isAttributed\n";
  for (size_t i = 0; i < rstAd.size(); i++) {
    content << rstAd.at(i) << "," << rstConv.at(i) << "," << rstLabel.at(i)
            << "\n";
  }
  fbpcf::io::FileIOWrappers::writeFile(FLAGS_output_file_path, content.str());

  cost.end();
  XLOG(INFO, cost.getEstimatedCostString());

  XLOG(INFO) << "output size:" << rstAd.size() << std::endl;

  auto gateStats = fbpcf::scheduler::SchedulerKeeper<0>::getGateStatistics();
  XLOG(INFO) << "Non-free gate count: " << gateStats.first << '\n';
  XLOG(INFO) << "Free gate count: " << gateStats.second << '\n';

  auto trafficStats =
      fbpcf::scheduler::SchedulerKeeper<0>::getTrafficStatistics();
  XLOG(INFO) << "Tx bytes: " << trafficStats.first << '\n';
  XLOG(INFO) << "Rx bytes: " << trafficStats.second << '\n';

  if (FLAGS_log_cost) {
    bool run_name_specified = FLAGS_run_name != "";
    auto run_name = run_name_specified ? FLAGS_run_name : "temp_run_name";
    auto party = (FLAGS_party == PUBLISHER_ROLE) ? "Publisher" : "Partner";

    folly::dynamic extra_info = folly::dynamic::object(
        "publisher_input_path", (FLAGS_party == PUBLISHER_ROLE) ? FLAGS_input_file_path : "")
        ("partner_input_basepath", (FLAGS_party == PARTNER_ROLE) ? FLAGS_input_file_path : "")
        ("publisher_output_basepath", (FLAGS_party == PUBLISHER_ROLE) ? FLAGS_output_file_path : "")
        ("partner_output_basepath", (FLAGS_party == PARTNER_ROLE) ? FLAGS_output_file_path : "")
        ("non_free_gates", gateStats.first)
        ("free_gates", gateStats.second)
        ("scheduler_transmitted_network", trafficStats.first)
        ("scheduler_received_network", trafficStats.second)
        ("mpc_traffic_details", commAgentFactory->getMetricsCollector()->collectMetrics());

    folly::dynamic costDict =
        cost.getEstimatedCostDynamic(run_name, party, extra_info);

    auto objectName = run_name_specified
        ? run_name
        : folly::to<std::string>(
              run_name, '_', costDict["timestamp"].asString());

    XLOGF(INFO, "{}", cost.writeToS3(party, objectName, costDict));
  }

  return 0;
}
