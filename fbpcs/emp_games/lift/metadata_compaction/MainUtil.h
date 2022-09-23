/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <memory>

#include "fbpcf/engine/communication/SocketPartyCommunicationAgentFactory.h"
#include "fbpcs/emp_games/lift/metadata_compaction/DummyMetadataCompactorGame.h"
#include "fbpcs/emp_games/lift/metadata_compaction/DummyMetadataCompactorGameFactory.h"
#include "fbpcs/emp_games/lift/metadata_compaction/MetadataCompactorApp.h"

namespace private_lift {

template <int PARTY>
inline common::SchedulerStatistics startMetadataCompactionApp(
    const std::string& inputFilepath,
    const std::string& outputGlobalParamsPath,
    const std::string& outputSecretSharesPath,
    std::string serverIp,
    int port,
    int numConversionsPerUser,
    bool computePublisherBreakdowns,
    int epoch,
    bool useXorEncryption,
    fbpcf::engine::communication::SocketPartyCommunicationAgent::TlsInfo&
        tlsInfo) {
  std::map<
      int,
      fbpcf::engine::communication::SocketPartyCommunicationAgentFactory::
          PartyInfo>
      partyInfos({{0, {serverIp, port}}, {1, {serverIp, port}}});

  auto communicationAgentFactory = std::make_unique<
      fbpcf::engine::communication::SocketPartyCommunicationAgentFactory>(
      PARTY, partyInfos, tlsInfo, "metadata_compaction_traffic");

  auto compactorGameFactory =
      std::make_unique<DummyMetadataCompactorGameFactory<PARTY>>();

  MetadataCompactorApp<PARTY> app(
      PARTY,
      std::move(communicationAgentFactory),
      std::move(compactorGameFactory),
      numConversionsPerUser,
      computePublisherBreakdowns,
      epoch,
      inputFilepath,
      outputGlobalParamsPath,
      outputSecretSharesPath,
      useXorEncryption);

  app.run();
  return app.getSchedulerStatistics();
}

} // namespace private_lift
