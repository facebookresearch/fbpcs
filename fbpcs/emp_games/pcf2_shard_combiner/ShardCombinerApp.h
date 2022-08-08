/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <cstdint>
#include <string>
#include <vector>

#include <folly/dynamic.h>
#include <folly/json.h>

#include <fbpcf/engine/communication/IPartyCommunicationAgent.h>
#include <fbpcf/engine/communication/IPartyCommunicationAgentFactory.h>
#include <fbpcf/io/api/FileIOWrappers.h>
#include <fbpcf/scheduler/IScheduler.h>
#include <fbpcf/scheduler/SchedulerHelper.h>

#include "fbpcs/emp_games/common/Constants.h"
#include "fbpcs/emp_games/common/SchedulerStatistics.h"
#include "fbpcs/emp_games/pcf2_shard_combiner/AggMetrics.h"
#include "fbpcs/emp_games/pcf2_shard_combiner/AggMetrics_impl.h"
#include "fbpcs/emp_games/pcf2_shard_combiner/ShardCombinerGame.h"
#include "fbpcs/emp_games/pcf2_shard_combiner/ShardValidator.h"
#include "fbpcs/emp_games/pcf2_shard_combiner/ShardValidator_impl.h"

namespace shard_combiner {

template <
    ShardSchemaType shardSchemaType,
    int32_t schedulerId,
    bool usingBatch,
    common::InputEncryption inputEncryption>
class ShardCombinerApp {
 public:
  ShardCombinerApp(
      std::unique_ptr<
          fbpcf::engine::communication::IPartyCommunicationAgentFactory>
          communicationAgentFactory,
      const std::int32_t numShards,
      std::uint32_t shardStartIndex,
      const std::string& inputPath,
      const std::string& inputFilePrefix,
      const std::string& outputPath,
      std::int64_t threshold,
      bool useXorEncryption,
      common::ResultVisibility resultVisibility)
      : shardStartIndex_(shardStartIndex),
        numShards_(numShards),
        threshold_(threshold),
        inputPath_(inputPath),
        inputFilePrefix_(inputFilePrefix),
        outputPath_(outputPath),
        resultVisibility_(resultVisibility),
        communicationAgentFactory_(std::move(communicationAgentFactory)),
        useXorEncryption_(useXorEncryption),
        schedulerStatistics_{0, 0, 0, 0, 0} {
    XLOG(INFO) << "Instantiated: " << schedulerId;
  }

  void run() {
    auto scheduler = useXorEncryption_
        ? fbpcf::scheduler::createLazySchedulerWithRealEngine(
              schedulerId, *communicationAgentFactory_)
        : fbpcf::scheduler::createNetworkPlaintextScheduler<true /*unsafe*/>(
              schedulerId, *communicationAgentFactory_);
    auto metricsCollector = communicationAgentFactory_->getMetricsCollector();

    XLOG(INFO) << "Made scheduler: " << schedulerId;

    ShardCombinerGame<shardSchemaType, schedulerId, usingBatch, inputEncryption>
        game(std::move(scheduler), std::move(communicationAgentFactory_));

    XLOG(INFO) << "Constructed game obj for: " << schedulerId;

    // read shards in the game and populate secret vals
    auto inputs = game.readShards(inputPath_, inputFilePrefix_, numShards_);

    XLOG(INFO) << "Read input files: " << inputPath_ << "/" << inputFilePrefix_;

    XLOG(INFO) << "Starting the Game: " << schedulerId;
    auto resSecret = game.play(inputs);
    XLOG(INFO) << "Playing: " << inputPath_ << "/" << inputFilePrefix_;

    std::unordered_map<int32_t, folly::dynamic> ret;

    // Insert revealed results only if the party has access for the result to be
    // revealed. Otherwise, insert dummy result.
    if (resultVisibility_ == common::ResultVisibility::kPublisher ||
        resultVisibility_ == common::ResultVisibility::kPublic) {
      ret.insert(std::make_pair(
          common::PUBLISHER, resSecret->toRevealedDynamic(common::PUBLISHER)));
    } else {
      auto dummyResult =
          AggMetrics<schedulerId, usingBatch, inputEncryption>::newLike(
              resSecret);
      ret.insert(std::make_pair(common::PUBLISHER, dummyResult->toDynamic()));
    }

    // Insert revealed results only if the party has access for the result to be
    // revealed. Otherwise, insert dummy result.
    if (resultVisibility_ == common::ResultVisibility::kPartner ||
        resultVisibility_ == common::ResultVisibility::kPublic) {
      ret.insert(std::make_pair(
          common::PARTNER, resSecret->toRevealedDynamic(common::PARTNER)));
    } else {
      auto dummyResult =
          AggMetrics<schedulerId, usingBatch, inputEncryption>::newLike(
              resSecret);
      ret.insert(std::make_pair(common::PARTNER, dummyResult->toDynamic()));
    }

    // Write only owner Party's output
    putOutputData(ret.at(schedulerId));

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
  }

  common::SchedulerStatistics getSchedulerStatistics() {
    return schedulerStatistics_;
  }

 protected:
  void putOutputData(
      const AggMetrics_sp<schedulerId, usingBatch, inputEncryption>&
          outputData) {
    fbpcf::io::FileIOWrappers::writeFile(
        outputPath_, folly::toJson(outputData->toDynamic()));
  }
  void putOutputData(const folly::dynamic& outputData) {
    fbpcf::io::FileIOWrappers::writeFile(
        outputPath_, folly::toJson(outputData));
  }

 private:
  int32_t shardStartIndex_;
  int32_t numShards_;
  int64_t threshold_;
  std::string inputPath_;
  std::string inputFilePrefix_;
  std::string outputPath_;
  common::ResultVisibility resultVisibility_;

  std::unique_ptr<fbpcf::engine::communication::IPartyCommunicationAgentFactory>
      communicationAgentFactory_;
  bool useXorEncryption_;
  common::SchedulerStatistics schedulerStatistics_;
};
} // namespace shard_combiner
