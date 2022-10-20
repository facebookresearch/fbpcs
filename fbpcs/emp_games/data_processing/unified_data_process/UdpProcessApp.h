/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <memory>
#include "fbpcf/engine/communication/IPartyCommunicationAgentFactory.h"
#include "fbpcf/scheduler/IScheduler.h"
#include "fbpcs/emp_games/common/SchedulerStatistics.h"
#include "fbpcs/emp_games/data_processing/unified_data_process/UdpProcessGame.h"
#include "fbpcs/emp_games/data_processing/unified_data_process/UdpProcessGameFactory.h"
#include "fbpcs/performance_tools/CostEstimation.h"

namespace unified_data_process {

template <int schedulerId>
class UdpProcessApp {
 public:
  UdpProcessApp(
      int party,
      std::shared_ptr<
          fbpcf::engine::communication::IPartyCommunicationAgentFactory>
          communicationAgentFactory,
      std::shared_ptr<fbpcf::util::MetricCollector> metricCollector,
      std::unique_ptr<UdpProcessGameFactory<schedulerId>> udpGameFactory,
      int32_t numberOfRows,
      int64_t sizeOfRow,
      int32_t numberOfIntersection,
      std::shared_ptr<fbpcs::performance_tools::CostEstimation> costEst,
      bool useXorEncryption = true)
      : party_{party},
        communicationAgentFactory_{std::move(communicationAgentFactory)},
        metricCollector_{std::move(metricCollector)},
        udpGameFactory_{std::move(udpGameFactory)},
        numberOfRows_{numberOfRows},
        sizeOfRow_{sizeOfRow},
        numberOfIntersection_{numberOfIntersection},
        costEst_{costEst},
        useXorEncryption_{useXorEncryption} {}

  // return the extracted shares of intersected metadata from publiser and
  // partner
  std::tuple<std::vector<std::vector<bool>>, std::vector<std::vector<bool>>>
  run();

  common::SchedulerStatistics getSchedulerStatistics() {
    return schedulerStatistics_;
  }

 protected:
  std::unique_ptr<fbpcf::scheduler::IScheduler> createScheduler();

  std::tuple<std::vector<int32_t>, std::vector<std::vector<unsigned char>>>
  dataGeneration();

 private:
  int party_;
  std::shared_ptr<fbpcf::engine::communication::IPartyCommunicationAgentFactory>
      communicationAgentFactory_;
  std::shared_ptr<fbpcf::util::MetricCollector> metricCollector_;
  std::unique_ptr<UdpProcessGameFactory<schedulerId>> udpGameFactory_;
  int64_t numberOfRows_;
  int64_t sizeOfRow_;
  int64_t numberOfIntersection_;
  std::shared_ptr<fbpcs::performance_tools::CostEstimation> costEst_;
  bool useXorEncryption_;
  common::SchedulerStatistics schedulerStatistics_;
};

} // namespace unified_data_process

#include "fbpcs/emp_games/data_processing/unified_data_process/UdpProcessApp_impl.h"
