/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "ShardAggregatorApp.h"

#include <memory>
#include <string>
#include <vector>

#include <emp-sh2pc/emp-sh2pc.h>
#include <folly/Format.h>
#include <folly/json.h>
#include <folly/logging/xlog.h>
#include "folly/Conv.h"

#include <fbpcf/common/FunctionalUtil.h>
#include <fbpcf/io/FileManagerUtil.h>
#include <fbpcf/io/api/FileIOWrappers.h>
#include <fbpcf/mpc/EmpGame.h>
#include "AggMetrics.h"
#include "ShardAggregatorGame.h"
#include "ShardAggregatorValidation.h"
#include "fbpcs/emp_games/attribution/shard_aggregator/AggMetricsThresholdCheckers.h"

namespace measurement::private_attribution {
using AggMetrics = private_measurement::AggMetrics;
using AggMetricsTag = private_measurement::AggMetricsTag;

void ShardAggregatorApp::run() {
  auto inputData = getInputData();

  auto io = std::make_unique<emp::NetIO>(
      party_ == fbpcf::Party::Alice ? nullptr : serverIp_.c_str(),
      port_,
      true /* quiet mode */);

  XLOG(INFO) << "NetIO is connected.";

  std::function<void(std::shared_ptr<AggMetrics>)> thresholdChecker;
  if (metricsFormatType_ == "lift") {
    thresholdChecker = constructLiftThresholdChecker(threshold_);

  } else if (metricsFormatType_ == "ad_object") {
    thresholdChecker = constructAdObjectFormatThresholdChecker(threshold_);

  } else {
    XLOG(FATAL) << "invalid format type " << metricsFormatType_
                << " passed to aggregator";
  }

  ShardAggregatorGame game{
      std::move(io), party_, thresholdChecker, visibility_};
  auto encryptedResult = game.perfPlay(inputData);

  auto result = revealMetrics(encryptedResult);
  putOutputData(result);
};

std::vector<std::string> ShardAggregatorApp::getInputPaths(
    const std::string& inputPath,
    const int firstShardIndex,
    const int numShards) {
  std::vector<std::string> v;

  for (int i = firstShardIndex; i < firstShardIndex + numShards; i++) {
    v.push_back(folly::sformat("{}_{}", inputPath, i));
  }

  return v;
}

std::vector<std::shared_ptr<AggMetrics>> ShardAggregatorApp::getInputData() {
  XLOG(INFO) << "getting input data ...";
  auto inputPaths = ShardAggregatorApp::getInputPaths(
      inputPath_, firstShardIndex_, numShards_);

  auto inputData =
      fbpcf::functional::map<std::string, std::shared_ptr<AggMetrics>>(
          inputPaths, [](const auto& inputPath) {
            XLOG(INFO) << "Opening file at <" << inputPath << ">";
            return std::make_shared<AggMetrics>(
                AggMetrics::fromDynamic(folly::parseJson(
                    fbpcf::io::FileIOWrappers::readFile(inputPath))));
          });
  validateInputDataAggMetrics(inputData, metricsFormatType_);
  return inputData;
}

void ShardAggregatorApp::putOutputData(
    const std::shared_ptr<AggMetrics>& metrics) {
  XLOG(INFO) << "putting out data ...";
  fbpcf::io::FileIOWrappers::writeFile(
      outputPath_, folly::toJson(metrics->toDynamic()));
}

std::shared_ptr<AggMetrics> ShardAggregatorApp::revealMetrics(
    const std::shared_ptr<AggMetrics>& metrics) {
  switch (metrics->getTag()) {
    case AggMetricsTag::Map: {
      auto revealedMetrics = std::make_shared<AggMetrics>(AggMetricsTag::Map);
      for (const auto& [key, value] : metrics->getAsMap()) {
        revealedMetrics->emplace(key, revealMetrics(value));
      }
      return revealedMetrics;
    }
    case AggMetricsTag::List: {
      auto revealedMetrics = std::make_shared<AggMetrics>(AggMetricsTag::List);
      for (const auto& m : metrics->getAsList()) {
        revealedMetrics->pushBack(revealMetrics(m));
      }
      return revealedMetrics;
    }
    case AggMetricsTag::EmpInteger: {
      return std::make_shared<AggMetrics>(
          AggMetrics{metrics->getEmpIntValue().reveal<int64_t>(
              static_cast<int32_t>(visibility_))});
    }
    default: {
      XLOG(FATAL)
          << "AggMetrics should only store a map, list, or emp::Integer at this point";
    }
  }
}
} // namespace measurement::private_attribution
