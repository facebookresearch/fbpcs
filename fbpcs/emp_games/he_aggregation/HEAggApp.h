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

#include "fbpcs/emp_games/he_aggregation/AggregationInputMetrics.h"
#include "fbpcs/emp_games/he_aggregation/HEAggGame.h"

#include <folly/json.h>

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
      double delta,
      double eps,
      common::InputEncryption inputEncryption,
      bool addDpNoise = true)
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

    XLOG(INFO) << "Start Reading input file ";

    AggregationInputMetrics input = getInputData(
        common::InputEncryption::Plaintext,
        secretShareFilePath_,
        inputFilePath_);

    auto touchpointMetadataArrays = input.getTouchpointMetadata();
    auto secretShareAttributionArrays = input.getAttributionSecretShares();

    XLOGF(
        INFO,
        "Touchpoint Array size = {}, Secret share attribution array size = {}",
        touchpointMetadataArrays.size(),
        secretShareAttributionArrays[0].size());

    XLOG(INFO) << "Finished Reading input file ";

    HEAggGame game(std::move(communicationAgentFactory_));

    std::unordered_map<uint64_t, uint64_t> output =
        game.computeAggregations(MY_ROLE, input);

    if (MY_ROLE == common::PUBLISHER) {
      XLOG(INFO, "Writing output ...");
      writeOutputData(output, outputFilePath_);
    }
    schedulerStatistics_.details = metricCollector_->collectMetrics();
  }

  common::SchedulerStatistics getSchedulerStatistics() {
    return schedulerStatistics_;
  }

  AggregationInputMetrics getInputData(
      common::InputEncryption inputEncryption,
      std::string inputSecretShareFilePath,
      std::string inputClearTextFilePath) {
    XLOGF(
        INFO,
        "input_secret_share_file_path = {},  input_clear_text_file_path = {}",
        inputSecretShareFilePath,
        inputClearTextFilePath);

    return AggregationInputMetrics{
        inputEncryption, inputSecretShareFilePath, inputClearTextFilePath};
  }
  void writeOutputData(
      const std::unordered_map<uint64_t, uint64_t> output,
      const std::string outputPath) {
    folly::dynamic res = folly::dynamic::object();
    for (auto i = output.begin(); i != output.end(); i++) {
      res.insert(std::to_string(i->first), std::to_string(i->second));
    }
    fbpcf::io::FileIOWrappers::writeFile(outputPath, folly::toJson(res));
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
