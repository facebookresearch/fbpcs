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
  auto scheduler = createScheduler();

  auto metricsCollector = communicationAgentFactory_->getMetricsCollector();

  InputData inputData(
      inputPath_,
      InputData::LiftMPCType::Standard,
      computePublisherBreakdowns_,
      epoch_,
      numConversionsPerUser_);

  auto metadataCompactorGame =
      compactorGameFactory_->create(std::move(scheduler), party_);

  auto inputProcessor =
      metadataCompactorGame->play(inputData, numConversionsPerUser_);
  writeToCSV(*inputProcessor, outputGlobalParamsPath_, outputSecretSharesPath_);

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
