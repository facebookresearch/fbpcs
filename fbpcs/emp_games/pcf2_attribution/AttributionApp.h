/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <fbpcf/io/FileManagerUtil.h>

#include "fbpcf/engine/communication/IPartyCommunicationAgentFactory.h"
#include "fbpcf/scheduler/SchedulerHelper.h"
#include "fbpcs/emp_games/common/SchedulerStatistics.h"
#include "fbpcs/emp_games/pcf2_attribution/AttributionGame.h"

namespace pcf2_attribution {

template <
    int MY_ROLE,
    int schedulerId,
    bool usingBatch,
    common::InputEncryption inputEncryption>
class AttributionApp {
 public:
  AttributionApp(
      std::unique_ptr<
          fbpcf::engine::communication::IPartyCommunicationAgentFactory>
          communicationAgentFactory,
      const std::string& attributionRules,
      const std::vector<std::string>& inputFilenames,
      const std::vector<std::string>& outputFilenames,
      std::uint32_t startFileIndex = 0U,
      int numFiles = 1,
      bool useNewOutputFormat = false)
      : communicationAgentFactory_(std::move(communicationAgentFactory)),
        attributionRules_{attributionRules},
        inputFilenames_(inputFilenames),
        outputFilenames_(outputFilenames),
        startFileIndex_(startFileIndex),
        numFiles_(numFiles),
        useNewOutputFormat_{useNewOutputFormat},
        schedulerStatistics_{0, 0, 0, 0} {}

  void run() {
    auto scheduler = fbpcf::scheduler::createLazySchedulerWithRealEngine(
        MY_ROLE, *communicationAgentFactory_);

    AttributionGame<schedulerId, usingBatch, inputEncryption> game(
        std::move(scheduler));

    // Compute attributions sequentially on numFiles files, starting from
    // startFileIndex
    for (size_t i = startFileIndex_; i < startFileIndex_ + numFiles_; ++i) {
      CHECK_LT(i, inputFilenames_.size())
          << "File index exceeds number of files.";
      auto inputData = getInputData(inputFilenames_.at(i));
      auto output =
          game.computeAttributions(MY_ROLE, inputData, useNewOutputFormat_);
      putOutputData(output, outputFilenames_.at(i));
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
  }

  common::SchedulerStatistics getSchedulerStatistics() {
    return schedulerStatistics_;
  }

 protected:
  AttributionInputMetrics<usingBatch, inputEncryption> getInputData(
      std::string inputPath) {
    XLOG(INFO) << "MY_ROLE: " << MY_ROLE << ", schedulerId: " << schedulerId
               << ", attributionRules_: " << attributionRules_
               << ", input_path: " << inputPath;
    return AttributionInputMetrics<usingBatch, inputEncryption>{
        MY_ROLE, attributionRules_, inputPath};
  }

  void putOutputData(
      const AttributionOutputMetrics& attributions,
      std::string outputPath) {
    fbpcf::io::write(outputPath, attributions.toJson());
  }

 private:
  std::unique_ptr<fbpcf::engine::communication::IPartyCommunicationAgentFactory>
      communicationAgentFactory_;
  std::string attributionRules_;
  std::vector<std::string> inputFilenames_;
  std::vector<std::string> outputFilenames_;
  const std::uint32_t startFileIndex_;
  const int numFiles_;
  const bool useNewOutputFormat_;
  common::SchedulerStatistics schedulerStatistics_;
};

} // namespace pcf2_attribution
