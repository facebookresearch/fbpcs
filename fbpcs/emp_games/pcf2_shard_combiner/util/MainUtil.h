/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <cstdint>
#include <memory>

#include <fbpcf/engine/communication/SocketPartyCommunicationAgentFactory.h>

#include <fbpcs/emp_games/common/SchedulerStatistics.h>
#include "fbpcs/emp_games/common/Constants.h"
#include "fbpcs/emp_games/pcf2_shard_combiner/ShardCombinerApp.h"
#include "fbpcs/emp_games/pcf2_shard_combiner/ShardValidator.h"

namespace shard_combiner {

template <ShardSchemaType shardSchemaType>
common::SchedulerStatistics runApp(
    int32_t schedulerId,
    bool usingBatch,
    common::InputEncryption inputEncryption,
    std::int32_t numShards,
    std::uint32_t shardStartIndex,
    const std::string& inputPath,
    const std::string& inputFilePrefix,
    const std::string& outputPath,
    std::int64_t threshold,
    bool useTls,
    const std::string& tlsDir,
    bool useXorEncryption,
    int32_t visibility,
    std::string ip,
    std::uint16_t port) {
  assert(inputEncryption == common::InputEncryption::Xor);
  assert(visibility == 0 || visibility == 1 || visibility == 2);

  common::ResultVisibility resultVisibility;
  switch (visibility) {
    case 0:
      resultVisibility = common::ResultVisibility::kPublic;
      break;
    case 1:
      resultVisibility = common::ResultVisibility::kPublisher;
      break;
    case 2:
      resultVisibility = common::ResultVisibility::kPartner;
      break;
  }

  std::map<
      int32_t,
      fbpcf::engine::communication::SocketPartyCommunicationAgentFactory::
          PartyInfo>
      partyInfos(
          {{common::PUBLISHER, {ip, port}}, {common::PARTNER, {ip, port}}});

  auto communicationAgentFactory = std::make_unique<
      fbpcf::engine::communication::SocketPartyCommunicationAgentFactory>(
      schedulerId, partyInfos, useTls, tlsDir, "shard_combiner_traffic");

  common::SchedulerStatistics schedulerStats;
  if (schedulerId == common::PUBLISHER) {
    if (usingBatch) {
      auto app = std::make_unique<ShardCombinerApp<
          shardSchemaType,
          common::PUBLISHER,
          true /* usingBatch */,
          common::InputEncryption::Xor>>(
          std::move(communicationAgentFactory),
          numShards,
          shardStartIndex,
          inputPath,
          inputFilePrefix,
          outputPath,
          threshold,
          useXorEncryption,
          resultVisibility);
      app->run();
      return app->getSchedulerStatistics();
    } else {
      auto app = std::make_unique<ShardCombinerApp<
          shardSchemaType,
          common::PUBLISHER,
          false /* usingBatch */,
          common::InputEncryption::Xor>>(
          std::move(communicationAgentFactory),
          numShards,
          shardStartIndex,
          inputPath,
          inputFilePrefix,
          outputPath,
          threshold,
          useXorEncryption,
          resultVisibility);
      app->run();
      return app->getSchedulerStatistics();
    }
  } else if (schedulerId == common::PARTNER) {
    if (usingBatch) {
      auto app = std::make_unique<ShardCombinerApp<
          shardSchemaType,
          common::PARTNER,
          true /* usingBatch */,
          common::InputEncryption::Xor>>(
          std::move(communicationAgentFactory),
          numShards,
          shardStartIndex,
          inputPath,
          inputFilePrefix,
          outputPath,
          threshold,
          useXorEncryption,
          resultVisibility);
      app->run();
      return app->getSchedulerStatistics();
    } else {
      auto app = std::make_unique<ShardCombinerApp<
          shardSchemaType,
          common::PARTNER,
          false /* usingBatch */,
          common::InputEncryption::Xor>>(
          std::move(communicationAgentFactory),
          numShards,
          shardStartIndex,
          inputPath,
          inputFilePrefix,
          outputPath,
          threshold,
          useXorEncryption,
          resultVisibility);
      app->run();
      return app->getSchedulerStatistics();
    }
  }
  return {};
}

} // namespace shard_combiner
