/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <string>
#include <vector>

#include <fbpcf/mpc/EmpApp.h>
#include <fbpcf/mpc/EmpGame.h>
#include "AggMetrics.h"
#include "ShardAggregatorGame.h"
#include "fbpcs/emp_games/attribution/shard_aggregator/AggMetrics.h"

namespace measurement::private_attribution {
class ShardAggregatorApp
    : public fbpcf::EmpApp<
          ShardAggregatorGame<emp::NetIO>,
          std::vector<std::shared_ptr<private_measurement::AggMetrics>>,
          std::shared_ptr<private_measurement::AggMetrics>> {
 public:
  ShardAggregatorApp(
      fbpcf::Party party,
      fbpcf::Visibility visibility,
      const std::string& serverIp,
      uint16_t port,
      int32_t firstShardIndex,
      int32_t numShards,
      int64_t threshold,
      const std::string& inputPath,
      const std::string& outputPath,
      const std::string& metricsFormatType = "ad_object")
      : fbpcf::EmpApp<
            ShardAggregatorGame<emp::NetIO>,
            std::vector<std::shared_ptr<private_measurement::AggMetrics>>,
            std::shared_ptr<
                private_measurement::AggMetrics>>{party, serverIp, port},
        firstShardIndex_{firstShardIndex},
        numShards_{numShards},
        threshold_{threshold},
        inputPath_{inputPath},
        outputPath_{outputPath},
        visibility_{visibility},
        metricsFormatType_{metricsFormatType} {}

  void run() override;

 protected:
  std::vector<std::shared_ptr<private_measurement::AggMetrics>> getInputData()
      override;
  void putOutputData(const std::shared_ptr<private_measurement::AggMetrics>&
                         outputData) override;

 private:
  static std::vector<std::string> getInputPaths(
      const std::string& inputPath,
      int32_t firstShardIndex,
      int32_t numShards);
  std::shared_ptr<private_measurement::AggMetrics> revealMetrics(
      const std::shared_ptr<private_measurement::AggMetrics>& metrics);

 private:
  int32_t firstShardIndex_;
  int32_t numShards_;
  int64_t threshold_;
  std::string inputPath_;
  std::string outputPath_;
  fbpcf::Visibility visibility_;
  std::string metricsFormatType_;
};
} // namespace measurement::private_attribution
