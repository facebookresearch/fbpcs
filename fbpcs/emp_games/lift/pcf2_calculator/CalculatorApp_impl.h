/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <fbpcf/io/api/FileIOWrappers.h>
#include <fbpcf/scheduler/SchedulerHelper.h>
#include <vector>

namespace private_lift {

template <int schedulerId>
void CalculatorApp<schedulerId>::run() {
  // Run calculator game sequentially on numFiles files, starting from
  // startFileIndex
  auto scheduler = createScheduler();
  auto metricsCollector = communicationAgentFactory_->getMetricsCollector();
  CalculatorGame<schedulerId> game{
      party_, std::move(scheduler), std::move(communicationAgentFactory_)};

  for (size_t i = startFileIndex_; i < startFileIndex_ + numFiles_; ++i) {
    try {
      CHECK_LT(i, inputPaths_.size()) << "File index exceeds number of files.";
      CalculatorGameConfig config = getInputData(inputPaths_.at(i));
      auto numRows = config.inputData.getNumRows();
      XLOG(INFO) << "Have " << numRows << " values in inputData.";
      auto output = game.play(config);
      XLOG(INFO) << "done calculating";
      putOutputData(output, outputPaths_.at(i));
    } catch (const std::exception& e) {
      XLOGF(
          ERR,
          "Error: Exception caught in CalculatorApp run.\n \t error msg: {} \n \t input shard: {}.",
          e.what(),
          inputPaths_.at(i));
      std::exit(1);
    }
  }

  auto gateStatistics =
      fbpcf::scheduler::SchedulerKeeper<schedulerId>::getGateStatistics();
  XLOGF(
      INFO,
      "Non-free gate count = {}, Free gate count = {}",
      gateStatistics.first,
      gateStatistics.second);

  auto trafficStatistics =
      fbpcf::scheduler::SchedulerKeeper<schedulerId>::getTrafficStatistics();
  XLOGF(
      INFO,
      "Sent network traffic = {}, Received network traffic = {}",
      trafficStatistics.first,
      trafficStatistics.second);

  schedulerStatistics_.nonFreeGates = gateStatistics.first;
  schedulerStatistics_.freeGates = gateStatistics.second;
  schedulerStatistics_.sentNetwork = trafficStatistics.first;
  schedulerStatistics_.receivedNetwork = trafficStatistics.second;
  schedulerStatistics_.details = metricsCollector->collectMetrics();
};

template <int schedulerId>
CalculatorGameConfig CalculatorApp<schedulerId>::getInputData(
    const std::string& inputPath) {
  XLOG(INFO) << "Parsing input from " << inputPath;

  InputData inputData{
      inputPath,
      InputData::LiftMPCType::Standard,
      epoch_,
      numConversionsPerUser_};
  CalculatorGameConfig config = {inputData, true, numConversionsPerUser_};
  return config;
}

template <int schedulerId>
void CalculatorApp<schedulerId>::putOutputData(
    const std::string& output,
    const std::string& outputPath) {
  XLOG(INFO) << "putting out data...";
  fbpcf::io::FileIOWrappers::writeFile(outputPath, output);
}

template <int schedulerId>
std::unique_ptr<fbpcf::scheduler::IScheduler>
CalculatorApp<schedulerId>::createScheduler() {
  return useXorEncryption_
      ? fbpcf::scheduler::createLazySchedulerWithRealEngine(
            party_, *communicationAgentFactory_)
      : fbpcf::scheduler::createNetworkPlaintextScheduler<false>(
            party_, *communicationAgentFactory_);
}
} // namespace private_lift
