/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <cstdint>
#include <future>
#include <memory>

#include <fbpcf/engine/communication/SocketPartyCommunicationAgent.h>
#include <folly/dynamic.h>
#include "fbpcf/engine/communication/SocketPartyCommunicationAgentFactory.h"
#include "fbpcs/emp_games/pcf2_attribution/AttributionApp.h"

namespace pcf2_attribution {

inline std::pair<std::vector<std::string>, std::vector<std::string>>
getIOFilenames(
    int32_t numFiles,
    std::string inputBasePath,
    std::string outputBasePath,
    int32_t fileStartIndex,
    bool use_postfix) {
  // get all input files (we have multiple files if they were sharded)
  std::vector<std::string> inputFilenames;
  std::vector<std::string> outputFilenames;

  try {
    // if multiple files used (sharding)
    if (use_postfix) {
      for (auto i = 0; i < numFiles; i++) {
        std::string inputPathName =
            folly::sformat("{}_{}", inputBasePath, (fileStartIndex + i));
        std::string outputPathName =
            folly::sformat("{}_{}", outputBasePath, (fileStartIndex + i));
        inputFilenames.push_back(inputPathName);
        outputFilenames.push_back(outputPathName);
      }
    } else {
      inputFilenames.push_back(inputBasePath);
      outputFilenames.push_back(outputBasePath);
    }

  } catch (const std::exception& e) {
    XLOG(ERR) << "Error: Exception caught in Attribution run.\n \t error msg: "
              << e.what() << "\n \t input directory: " << inputBasePath;
    std::exit(1);
  }
  return std::make_pair(inputFilenames, outputFilenames);
}

template <
    std::uint32_t PARTY,
    std::uint32_t index,
    common::InputEncryption inputEncryption>
inline common::SchedulerStatistics startAttributionAppsForShardedFilesHelper(
    bool useXorEncryption,
    std::uint32_t startFileIndex,
    std::uint32_t remainingThreads,
    std::string serverIp,
    int port,
    std::string attributionRules,
    std::vector<std::string>& inputFilenames,
    std::vector<std::string>& outputFilenames,
    fbpcf::engine::communication::SocketPartyCommunicationAgent::TlsInfo&
        tlsInfo) {
  // aggregate scheduler statistics across apps
  common::SchedulerStatistics schedulerStatistics{
      0, 0, 0, 0, folly::dynamic::object()};

  // split files evenly across threads
  auto remainingFiles =
      static_cast<std::int64_t>(inputFilenames.size()) - startFileIndex;
  if (remainingFiles > 0) {
    auto numFiles = (remainingThreads > remainingFiles)
        ? 1U
        : (remainingFiles / remainingThreads);

    std::map<
        int,
        fbpcf::engine::communication::SocketPartyCommunicationAgentFactory::
            PartyInfo>
        partyInfos(
            {{0, {serverIp, port + static_cast<int>(index) * 100}},
             {1, {serverIp, port + static_cast<int>(index) * 100}}});

    auto metricCollector = std::make_shared<fbpcf::util::MetricCollector>(
        "attribution_metrics_for_thread_" + std::to_string(index));

    auto communicationAgentFactory = std::make_unique<
        fbpcf::engine::communication::SocketPartyCommunicationAgentFactory>(
        PARTY, partyInfos, tlsInfo, metricCollector);

    // Each AttributionApp runs numFiles sequentially on a single thread
    // Publisher uses even schedulerId and partner uses odd schedulerId
    auto app = std::make_unique<
        pcf2_attribution::
            AttributionApp<PARTY, 2 * index + PARTY, true, inputEncryption>>(
        std::move(communicationAgentFactory),
        attributionRules,
        inputFilenames,
        outputFilenames,
        metricCollector,
        useXorEncryption,
        startFileIndex,
        numFiles);

    auto future = std::async([&app]() {
      app->run();
      return app->getSchedulerStatistics();
    });

    if constexpr (index < kMaxConcurrency) {
      if (remainingThreads > 1) {
        auto remainingStats = startAttributionAppsForShardedFilesHelper<
            PARTY,
            index + 1,
            inputEncryption>(
            useXorEncryption,
            startFileIndex + numFiles,
            remainingThreads - 1,
            serverIp,
            port,
            attributionRules,
            inputFilenames,
            outputFilenames,
            tlsInfo);
        schedulerStatistics.add(remainingStats);
      }
    }
    auto stats = future.get();
    schedulerStatistics.add(stats);
  }
  return schedulerStatistics;
}

template <int PARTY, common::InputEncryption inputEncryption>
inline common::SchedulerStatistics startAttributionAppsForShardedFiles(
    bool useXorEncryption,
    std::vector<std::string>& inputFilenames,
    std::vector<std::string>& outputFilenames,
    int16_t concurrency,
    const std::string& serverIp,
    int port,
    const std::string& attributionRules,
    fbpcf::engine::communication::SocketPartyCommunicationAgent::TlsInfo&
        tlsInfo) {
  // use only as many threads as the number of files
  auto numThreads =
      std::min(static_cast<std::int16_t>(inputFilenames.size()), concurrency);

  return startAttributionAppsForShardedFilesHelper<PARTY, 0U, inputEncryption>(
      useXorEncryption,
      0U,
      numThreads,
      serverIp,
      port,
      attributionRules,
      inputFilenames,
      outputFilenames,
      tlsInfo);
}

} // namespace pcf2_attribution
