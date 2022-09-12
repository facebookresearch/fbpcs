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

    XLOG(INFO) << "Start Reading input file ";
    auto inputTuple = readCSVInput(inputFilePath_, labelWidth_, numFeatures_);
    XLOG(INFO) << "Finished Reading input file ";

    XLOG(INFO) << "Number of feature rows " << std::get<0>(inputTuple).size();

    XLOG(INFO) << "Number of label rows " << std::get<1>(inputTuple).size();
    XLOG(INFO) << "Number of label columns "
               << std::get<1>(inputTuple).at(0).size();
  }

  common::SchedulerStatistics getSchedulerStatistics() {
    return schedulerStatistics_;
  }

 protected:
  std::tuple<std::vector<std::vector<double>>, std::vector<std::vector<bool>>>
  readCSVInput(std::string inputPath, int labelWidth, int numFeatures) {
    std::vector<std::vector<double>> allFeatures;
    std::vector<std::vector<bool>> allLabels;
    auto lineNo = 0;

    bool success = private_measurement::csv::readCsv(
        inputPath,
        [&](const std::vector<std::string>& header,
            const std::vector<std::string>& parts) {
          if (lineNo == 0) {
            XLOGF(DBG, "{}", common::vecToString(header));
          }

          auto [features, labels] =
              parseLine(lineNo, header, parts, labelWidth, numFeatures);

          // empty feature vector is not added for partner
          if (features.size() != 0)
            allFeatures.push_back(features);
          allLabels.push_back(labels);
          lineNo++;
        });

    return {allFeatures, transposeLabels(allLabels, labelWidth)};
  }

  std::tuple<std::vector<double>, std::vector<bool>> parseLine(
      const int lineNo,
      const std::vector<std::string>& header,
      const std::vector<std::string>& parts,
      int labelWidth,
      int numFeatures) {
    std::vector<double> features;
    std::vector<bool> labels(labelWidth);

    for (auto i = 0; i < header.size(); ++i) {
      auto column = header[i];

      if (column == "float_features") {
        if (i < parts.size()) {
          auto value = parts[i];
          features = common::getInnerArray<double>(value);
        } else {
          features = std::vector<double>(numFeatures);
        }

      } else if (column == "label_secret_share") {
        auto value = parts[i];
        for (int j = 0; j < value.size(); j++) {
          labels[j] = (value[j] == '1');
        }
      }
    }
    return {features, labels};
  }

  inline std::vector<std::vector<bool>> transposeLabels(
      std::vector<std::vector<bool>> labels,
      int labelWidth) {
    std::vector<std::vector<bool>> transposedLabels(
        labelWidth, std::vector<bool>(labels.size()));
    for (int i = 0; i < labels.size(); i++)
      for (int j = 0; j < labelWidth; j++)
        transposedLabels[j][i] = labels[i][j];
    return transposedLabels;
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
