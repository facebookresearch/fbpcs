/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <fbpcf/io/api/FileIOWrappers.h>

#include "fbpcf/engine/communication/IPartyCommunicationAgentFactory.h"
#include "fbpcf/scheduler/LazySchedulerFactory.h"
#include "fbpcs/emp_games/common/Constants.h"
#include "fbpcs/emp_games/common/Util.h"

#include "fbpcs/emp_games/common/SchedulerStatistics.h"

namespace pcf2_he {

template <int MY_ROLE, int schedulerId>
class HEAggApp {
 public:
  HEAggApp(
      std::unique_ptr<
          fbpcf::engine::communication::IPartyCommunicationAgentFactory>
          communicationAgentFactory,
      const std::string& secretShareFilePath,
      const std::string& inputFilePath,
      const std::string& outputFilePath,
      const std::shared_ptr<fbpcf::util::MetricCollector> metricCollector,
      const double delta,
      const double eps,
      const common::InputEncryption inputEncryption,
      const bool addDpNoise = true)
      : communicationAgentFactory_(std::move(communicationAgentFactory)),
        secretShareFilePath_(secretShareFilePath),
        inputFilePath_(inputFilePath),
        outputFilePath_(outputFilePath),
        delta_(delta),
        eps_(eps),
        schedulerStatistics_{0, 0, 0, 0, 0},
        metricCollector_{metricCollector},
        addDpNoise_(addDpNoise),
        inputEncryption_(inputEncryption) {}

  void run() {
    auto scheduler = fbpcf::scheduler::getLazySchedulerFactoryWithRealEngine(
                         MY_ROLE, *communicationAgentFactory_, metricCollector_)
                         ->create();

    XLOG(INFO) << "Start HEAgg App ";

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
    fbpcf::scheduler::SchedulerKeeper<schedulerId>::deleteEngine();
    schedulerStatistics_.nonFreeGates = gateStatistics.first;
    schedulerStatistics_.freeGates = gateStatistics.second;
    schedulerStatistics_.sentNetwork = trafficStatistics.first;
    schedulerStatistics_.receivedNetwork = trafficStatistics.second;
    schedulerStatistics_.details = metricCollector_->collectMetrics();
  }

  common::SchedulerStatistics getSchedulerStatistics() {
    return schedulerStatistics_;
  }

 private:
  std::unique_ptr<fbpcf::engine::communication::IPartyCommunicationAgentFactory>
      communicationAgentFactory_;
  std::string secretShareFilePath_;
  std::string inputFilePath_;
  std::string outputFilePath_;
  int numFeatures_;
  int labelWidth_;
  double delta_;
  double eps_;
  common::SchedulerStatistics schedulerStatistics_;
  std::shared_ptr<fbpcf::util::MetricCollector> metricCollector_;
  bool addDpNoise_;
  common::InputEncryption inputEncryption_;
};

} // namespace pcf2_he
