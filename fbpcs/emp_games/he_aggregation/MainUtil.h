/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <folly/dynamic.h>
#include <future>
#include <memory>

#include "fbpcf/engine/communication/SocketPartyCommunicationAgentFactory.h"
#include "fbpcs/emp_games/common/SchedulerStatistics.h"
#include "fbpcs/emp_games/he_aggregation/HEAggApp.h"

namespace pcf2_he {

template <int PARTY>
inline common::SchedulerStatistics startHEAggApp(
    const std::string& serverIp,
    const int port,
    const std::string& secretShareFilePath,
    const std::string& inputFilePath,
    const std::string& outFilePath,
    const double delta,
    const double eps,
    const bool addDpNoise,
    const fbpcf::engine::communication::SocketPartyCommunicationAgent::TlsInfo&
        tlsInfo,
    const common::InputEncryption inputEncryption) {
  std::map<
      int,
      fbpcf::engine::communication::SocketPartyCommunicationAgentFactory::
          PartyInfo>
      partyInfos({{0, {serverIp, port}}, {1, {serverIp, port}}});

  auto metricCollector =
      std::make_shared<fbpcf::util::MetricCollector>("heagg");

  auto communicationAgentFactory = std::make_unique<
      fbpcf::engine::communication::SocketPartyCommunicationAgentFactory>(
      PARTY, partyInfos, tlsInfo, metricCollector);

  ////////////////////////
  // Add HE Agg App Here
  ////////////////////////
  auto app = std::make_unique<pcf2_he::HEAggApp<PARTY, PARTY>>(
      std::move(communicationAgentFactory),
      secretShareFilePath,
      inputFilePath,
      outFilePath,
      metricCollector,
      delta,
      eps,
      inputEncryption,
      addDpNoise);

  app->run();
  return app->getSchedulerStatistics();

  common::SchedulerStatistics schedulerStatistics;
  return schedulerStatistics;
}

} // namespace pcf2_he
