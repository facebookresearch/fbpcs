/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <cstdio>
#include <memory>
#include <queue>
#include <string>

#include <fbpcf/engine/communication/SocketPartyCommunicationAgentFactory.h>
#include <fbpcf/io/api/BufferedReader.h>

#include "fbpcs/emp_games/common/Constants.h"
#include "fbpcs/emp_games/private_id_dfca_aggregator/util/ShardReader.h"
#include "fbpcs/emp_games/private_id_dfca_aggregator/util/SortedIdSwapper.h"

namespace private_id_dfca_aggregator {

class PrivateIdDfcaAggregatorApp {
 public:
  explicit PrivateIdDfcaAggregatorApp(
      std::unique_ptr<
          fbpcf::engine::communication::IPartyCommunicationAgentFactory>
          communicationAgentFactory);

  void run(
      const std::int8_t party,
      const std::string& inputPath,
      const std::string& outputPath);

 protected:
  void runPublisher(const std::string& outputPath);
  void runPartner();

 private:
  std::unique_ptr<fbpcf::engine::communication::IPartyCommunicationAgentFactory>
      communicationAgentFactory_;
  std::shared_ptr<ShardReader> shardReader_;
};
} // namespace private_id_dfca_aggregator
