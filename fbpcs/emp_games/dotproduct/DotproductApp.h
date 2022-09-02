/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <fbpcf/io/api/FileIOWrappers.h>

#include "fbpcf/engine/communication/IPartyCommunicationAgentFactory.h"
#include "fbpcf/scheduler/SchedulerHelper.h"
#include "fbpcs/emp_games/common/Constants.h"
#include "fbpcs/emp_games/common/Util.h"

#include "fbpcs/emp_games/common/SchedulerStatistics.h"

namespace pcf2_dotproduct {

template <int MY_ROLE, int schedulerId>
class DotproductApp {
 public:
  DotproductApp(
      std::unique_ptr<
          fbpcf::engine::communication::IPartyCommunicationAgentFactory>
          communicationAgentFactory,
      std::string& inputFilePath,
      std::string& outputFilePath,
      int numFeatures,
      int labelWidth,
      const bool debugMode = false)
      : communicationAgentFactory_(std::move(communicationAgentFactory)),
        inputFilePath_(inputFilePath),
        outputFilePath_(outputFilePath),
        numFeatures_(numFeatures),
        labelWidth_(labelWidth),
        schedulerStatistics_{0, 0, 0, 0, 0},
        debugMode_(debugMode) {}

  void run() {
    auto metricsCollector = communicationAgentFactory_->getMetricsCollector();

    auto scheduler = fbpcf::scheduler::createLazySchedulerWithRealEngine(
        MY_ROLE, *communicationAgentFactory_);

    XLOG(INFO) << "Starting Dotproduct App with role = " << MY_ROLE;
  }

  common::SchedulerStatistics getSchedulerStatistics() {
    return schedulerStatistics_;
  }

 private:
  std::unique_ptr<fbpcf::engine::communication::IPartyCommunicationAgentFactory>
      communicationAgentFactory_;
  std::string inputFilePath_;
  std::string outputFilePath_;
  int numFeatures_;
  int labelWidth_;
  common::SchedulerStatistics schedulerStatistics_;
  bool debugMode_;
};

} // namespace pcf2_dotproduct
