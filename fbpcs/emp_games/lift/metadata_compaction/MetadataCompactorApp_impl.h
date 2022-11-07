/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include "fbpcs/emp_games/lift/metadata_compaction/MetadataCompactorApp.h"

#include <fbpcf/scheduler/LazySchedulerFactory.h>
#include <fbpcf/scheduler/NetworkPlaintextSchedulerFactory.h>
#include "fbpcs/emp_games/lift/pcf2_calculator/input_processing/IInputProcessor.h"
#include "fbpcs/emp_games/lift/pcf2_calculator/input_processing/InputData.h"

namespace private_lift {

template <int schedulerId>
void MetadataCompactorApp<schedulerId>::run() {
  // first communication agent created
  auto scheduler = createScheduler();

  auto metricsCollector = communicationAgentFactory_->getMetricsCollector();

  // second communication agent created
  auto metadataCompactorGame =
      compactorGameFactory_->create(std::move(scheduler), party_);

  for (size_t i = startFileIndex_; i < startFileIndex_ + numFiles_; i++) {
    try {
      CHECK_LT(i, inputPaths_.size()) << "File index exceeds number of files.";
      CHECK_LT(i, outputGlobalParamsPaths_.size())
          << "File index exceeds number of files.";
      CHECK_LT(i, outputSecretSharesPaths_.size())
          << "File index exceeds number of files.";

      auto inputData = getInputData(inputPaths_.at(i));
      XLOG(INFO) << "Have " << inputData.getNumRows()
                 << " values in inputData.";
      auto inputProcessor =
          metadataCompactorGame->play(inputData, numConversionsPerUser_);
      XLOG(INFO) << "done calculating";
      writeToCSV(
          *inputProcessor,
          outputGlobalParamsPaths_.at(i),
          outputSecretSharesPaths_.at(i));
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
  fbpcf::scheduler::SchedulerKeeper<schedulerId>::deleteEngine();
  schedulerStatistics_.details = metricsCollector->collectMetrics();
}

template <int schedulerId>
InputData MetadataCompactorApp<schedulerId>::getInputData(
    const std::string& inputPath) {
  return InputData(
      inputPath,
      InputData::LiftMPCType::Standard,
      computePublisherBreakdowns_,
      epoch_,
      numConversionsPerUser_);
}

template <int schedulerId>
std::unique_ptr<fbpcf::scheduler::IScheduler>
MetadataCompactorApp<schedulerId>::createScheduler() {
  return useXorEncryption_
      ? fbpcf::scheduler::getLazySchedulerFactoryWithRealEngine(
            party_, *communicationAgentFactory_)
            ->create()
      : fbpcf::scheduler::NetworkPlaintextSchedulerFactory<false>(
            party_, *communicationAgentFactory_)
            .create();
}

} // namespace private_lift
