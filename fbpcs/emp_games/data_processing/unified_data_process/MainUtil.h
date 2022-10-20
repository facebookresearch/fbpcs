/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include "fbpcf/engine/communication/SocketPartyCommunicationAgentFactory.h"
#include "fbpcs/emp_games/data_processing/unified_data_process/UdpProcessApp.h"
#include "fbpcs/emp_games/data_processing/unified_data_process/UdpProcessGame.h"
#include "fbpcs/emp_games/data_processing/unified_data_process/UdpProcessGameFactory.h"
namespace unified_data_process {

template <int PARTY>
inline common::SchedulerStatistics startUdpProcessApp(
    std::string serverIp,
    int port,
    int64_t numberOfRows,
    int64_t sizeOfRow,
    int64_t numberOfIntersection,
    std::shared_ptr<fbpcs::performance_tools::CostEstimation> costEst,
    bool useXorEncryption) {
  std::map<
      int,
      fbpcf::engine::communication::SocketPartyCommunicationAgentFactory::
          PartyInfo>
      partyInfos({{0, {serverIp, port}}, {1, {serverIp, port}}});

  auto metricCollector =
      std::make_shared<fbpcf::util::MetricCollector>("udp_metrics");

  auto communicationAgentFactory = std::make_shared<
      fbpcf::engine::communication::SocketPartyCommunicationAgentFactory>(
      PARTY, partyInfos, metricCollector);

  auto udpGameFactory = std::make_unique<UdpProcessGameFactory<PARTY>>(
      PARTY, *communicationAgentFactory);

  UdpProcessApp<PARTY> app(
      PARTY,
      std::move(communicationAgentFactory),
      std::move(metricCollector),
      std::move(udpGameFactory),
      numberOfRows,
      sizeOfRow,
      numberOfIntersection,
      costEst,
      useXorEncryption);

  app.run();
  return app.getSchedulerStatistics();
}

} // namespace unified_data_process
