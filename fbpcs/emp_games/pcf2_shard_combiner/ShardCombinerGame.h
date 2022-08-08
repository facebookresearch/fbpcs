/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <algorithm>
#include <cstdlib>
#include <memory>
#include <vector>

#include <folly/Format.h>
#include <folly/logging/xlog.h>

#include <fbpcf/common/VectorUtil.h>
#include <fbpcf/engine/communication/IPartyCommunicationAgent.h>
#include <fbpcf/engine/communication/IPartyCommunicationAgentFactory.h>
#include <fbpcf/frontend/mpcGame.h>

#include "fbpcs/emp_games/common/Constants.h"
#include "fbpcs/emp_games/pcf2_shard_combiner/AggMetrics.h"
#include "fbpcs/emp_games/pcf2_shard_combiner/ShardValidator.h"
#include "fbpcs/emp_games/pcf2_shard_combiner/ShardValidator_impl.h"
#include "fbpcs/emp_games/pcf2_shard_combiner/util/AggMetricsThresholdCheckers.h"
#include "fbpcs/emp_games/pcf2_shard_combiner/util/AggMetricsThresholdCheckers_impl.h"

namespace shard_combiner {

template <
    ShardSchemaType shardSchemaType,
    int schedulerId,
    bool usingBatch,
    common::InputEncryption inputEncryption>
class ShardCombinerGame : public fbpcf::frontend::MpcGame<schedulerId> {
  using AggMetrics_sp =
      std::shared_ptr<AggMetrics<schedulerId, usingBatch, inputEncryption>>;

 public:
  ShardCombinerGame(
      std::unique_ptr<fbpcf::scheduler::IScheduler> scheduler,
      std::shared_ptr<
          fbpcf::engine::communication::IPartyCommunicationAgentFactory>
          communicationAgentFactory,
      const int concurrency = 1)
      : fbpcf::frontend::MpcGame<schedulerId>(std::move(scheduler)),
        communicationAgentFactory_(std::move(communicationAgentFactory)),
        concurrency_(concurrency) {
    thresholdFn_ =
        checkThresholdAndUpdateMetric<schedulerId, usingBatch, inputEncryption>(
            shardSchemaType, kAnonymityThreshold, kHiddenMetricConstant);
  }

  static constexpr int64_t kHiddenMetricConstant = -1;
  static constexpr int64_t kAnonymityThreshold = 100;

  AggMetrics_sp play(std::vector<AggMetrics_sp>& inputData) {
    reducer(inputData);

    auto result = inputData.at(0); // reduced output is in the zeroth element.

    thresholdFn_(result);

    return result;
  }

  // parallel reducer
  /*
   * follows a tree reduction
   *  0   1   2   3   4   5
   *  |   |   |   |   |   | ==> step = 1
   *  + --|   + --|   + --|
   *  |       | ======|=======> step = 2
   *  +-------|       |
   *  |               |
   *  + --------------|=======> step = 4
   *  |
   *  v
   *  final sum would be held in the first element of the array.
   *
   * Since, MPC's lazy scheduler internally parallelizes the ops that don't have
   * dependencies, we don't have need to launch a thread pool to realize this.
   */
  void reducer(std::vector<AggMetrics_sp>& input) {
    for (int step = 1;
         step < (input.size() % 2 == 0 ? input.size() : input.size() + 1);
         step <<= 1) {
      for (size_t i = 0;
           i < (input.size() % 2 == 0 ? input.size() : input.size() + 1);
           i += 2 * step) {
        // check second element is not out of the input array.
        if ((i + step) < input.size())
          AggMetrics<schedulerId, usingBatch, inputEncryption>::accumulate(
              input.at(i), input.at(i + step));
      }
    }
  }

  // TODO: parallelize these reads, and  attach validation. That way things
  // are faster.
  std::vector<AggMetrics_sp>
  readShards(std::string inputDir, std::string filename, int32_t numShards) {
    shards_.clear();
    for (int i = 0; i < numShards; i++) {
      std::string fullPath = folly::sformat("{}/{}_{}", inputDir, filename, i);
      auto shard =
          AggMetrics<schedulerId, usingBatch, inputEncryption>::fromJson(
              fullPath);
      XLOG(INFO) << "parsed: " << fullPath;
      validateShardSchema<shardSchemaType>(*shard);
      XLOG(INFO) << "validated: " << fullPath;
      shard->updateAllSecVals();
      XLOG(INFO) << "updatedSecVals: " << fullPath;
      shards_.push_back(shard);
    }
    return shards_;
  }

 private:
  common::InputEncryption inputEncryption_;
  std::shared_ptr<fbpcf::engine::communication::IPartyCommunicationAgentFactory>
      communicationAgentFactory_;
  int concurrency_;
  std::vector<AggMetrics_sp> shards_;

  std::function<void(AggMetrics_sp)> thresholdFn_;
};
} // namespace shard_combiner
