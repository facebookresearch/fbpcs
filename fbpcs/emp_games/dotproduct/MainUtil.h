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
#include "fbpcs/emp_games/dotproduct/DotproductApp.h"

namespace pcf2_dotproduct {

template <int PARTY>
inline common::SchedulerStatistics startDotProductApp(
    std::string serverIp,
    int port,
    std::string& inputFilePath,
    std::string& outFilePath,
    int numFeatures,
    int labelWidth,
    bool debugMode) {
  std::map<
      int,
      fbpcf::engine::communication::SocketPartyCommunicationAgentFactory::
          PartyInfo>
      partyInfos({{0, {serverIp, port}}, {1, {serverIp, port}}});
  fbpcf::engine::communication::SocketPartyCommunicationAgent::TlsInfo tlsInfo;
  tlsInfo.certPath = "";
  tlsInfo.keyPath = "";
  tlsInfo.passphrasePath = "";
  tlsInfo.useTls = false;

  auto metricCollector =
      std::make_shared<fbpcf::util::MetricCollector>("dotproduct");

  auto communicationAgentFactory = std::make_unique<
      fbpcf::engine::communication::SocketPartyCommunicationAgentFactory>(
      PARTY, partyInfos, tlsInfo, metricCollector);

  auto app = std::make_unique<pcf2_dotproduct::DotproductApp<PARTY, PARTY>>(
      std::move(communicationAgentFactory),
      inputFilePath,
      outFilePath,
      numFeatures,
      labelWidth,
      metricCollector,
      debugMode);

  app->run();
  return app->getSchedulerStatistics();
}

} // namespace pcf2_dotproduct
