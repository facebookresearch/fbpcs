/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <memory>
#include "folly/Format.h"

#include "fbpcf/engine/communication/SocketPartyCommunicationAgentFactory.h"
#include "fbpcs/emp_games/lift/metadata_compaction/MetadataCompactorApp.h"
#include "fbpcs/emp_games/lift/metadata_compaction/MetadataCompactorGame.h"
#include "fbpcs/emp_games/lift/metadata_compaction/MetadataCompactorGameFactory.h"

namespace private_lift {

struct LiftMetadataCompactionFilePaths {
  std::vector<std::string> inputFilePaths;
  std::vector<std::string> outputGlobalParamsFilePaths;
  std::vector<std::string> outputSecretSharesFilePaths;
};

inline LiftMetadataCompactionFilePaths getIOFilepaths(
    // single threaded UDP args
    std::string inputPath,
    std::string outputGlobalParamsPath,
    std::string outputSecretSharesPath,
    // multithreaded UDP args
    std::string inputBasePath,
    std::string outputGlobalParamsBasePath,
    std::string outputSecretSharesBasePath,
    int32_t numFile,
    int32_t startIndex) {
  if (!(inputBasePath.empty() || outputGlobalParamsBasePath.empty() ||
        outputSecretSharesBasePath.empty())) {
    LiftMetadataCompactionFilePaths paths{{}, {}, {}};
    std::string inputBasePathPrefix = inputBasePath + "_";

    for (auto i = startIndex; i < startIndex + numFile; i++) {
      paths.inputFilePaths.push_back(folly::sformat("{}_{}", inputBasePath, i));
      paths.outputGlobalParamsFilePaths.push_back(
          folly::sformat("{}_{}", outputGlobalParamsBasePath, i));
      paths.outputSecretSharesFilePaths.push_back(
          folly::sformat("{}_{}", outputSecretSharesBasePath, i));
    }
    return paths;
  } else {
    return {{inputPath}, {outputGlobalParamsPath}, {outputSecretSharesPath}};
  }
}

template <int PARTY, int index>
inline common::SchedulerStatistics
startMetadataCompactionAppForShardedFileHelper(
    const std::vector<std::string>& inputFilePaths,
    const std::vector<std::string>& outputGlobalParamsPaths,
    const std::vector<std::string>& outputSecretSharesPaths,
    int startFileIndex,
    int remainingThreads,
    int numThreads,
    std::string serverIp,
    int port,
    int numConversionsPerUser,
    bool computePublisherBreakdowns,
    int epoch,
    bool useXorEncryption,
    fbpcf::engine::communication::SocketPartyCommunicationAgent::TlsInfo&
        tlsInfo) {
  // aggregate scheduler statistics across apps
  common::SchedulerStatistics schedulerStatistics{
      0, 0, 0, 0, folly::dynamic::object()};

  // split files evenly across threads
  auto remainingFiles = inputFilePaths.size() - startFileIndex;
  if (remainingFiles > 0) {
    auto numFiles = (remainingThreads > remainingFiles)
        ? 1
        : (remainingFiles / remainingThreads);

    std::map<
        int,
        fbpcf::engine::communication::SocketPartyCommunicationAgentFactory::
            PartyInfo>
        partyInfos(
            {{0, {serverIp, port + index * 100}},
             {1, {serverIp, port + index * 100}}});

    /** It is safe to use a shared pointer to the same factory rather than a
     * whole new factory as the usage order is consistent across parties
     * 1. App will create scheduler -> creates first communicationAgent
     * 2. App will create CompactorGame -> creates DataProcessor -> creates
     * second communicationAgent
     */
    auto communicationAgentFactory = std::make_shared<
        fbpcf::engine::communication::SocketPartyCommunicationAgentFactory>(
        PARTY, partyInfos, tlsInfo, "metadata_compaction_traffic");

    auto compactorGameFactory =
        std::make_unique<MetadataCompactorGameFactory<2 * index + PARTY>>(
            communicationAgentFactory);

    auto app = std::make_unique<MetadataCompactorApp<2 * index + PARTY>>(
        PARTY,
        std::move(communicationAgentFactory),
        std::move(compactorGameFactory),
        numConversionsPerUser,
        computePublisherBreakdowns,
        epoch,
        inputFilePaths,
        outputGlobalParamsPaths,
        outputSecretSharesPaths,
        startFileIndex,
        numFiles,
        useXorEncryption);

    auto future = std::async([&app]() {
      app->run();
      return app->getSchedulerStatistics();
    });

    // We construct a MetadataCompactorApp for each thread recursively because
    // each app has a different schedulerId, which is a template parameter
    if constexpr (index < kMaxConcurrency) {
      if (remainingThreads > 1) {
        auto remainingStats =
            startMetadataCompactionAppForShardedFileHelper<PARTY, index + 1>(
                inputFilePaths,
                outputGlobalParamsPaths,
                outputSecretSharesPaths,
                startFileIndex + numFiles,
                remainingThreads - 1,
                numThreads,
                serverIp,
                port,
                numConversionsPerUser,
                computePublisherBreakdowns,
                epoch,
                useXorEncryption,
                tlsInfo);
        schedulerStatistics.add(remainingStats);
      }
    }

    auto stats = future.get();
    schedulerStatistics.add(stats);
  }
  return schedulerStatistics;
}

template <int PARTY>
inline common::SchedulerStatistics startMetadataCompactionApp(
    const std::vector<std::string>& inputFilePaths,
    const std::vector<std::string>& outputGlobalParamsPaths,
    const std::vector<std::string>& outputSecretSharesPaths,
    int16_t concurrency,
    std::string serverIp,
    int port,
    int numConversionsPerUser,
    bool computePublisherBreakdowns,
    int epoch,
    bool useXorEncryption,
    fbpcf::engine::communication::SocketPartyCommunicationAgent::TlsInfo&
        tlsInfo) {
  auto numThreads = std::min((int)inputFilePaths.size(), (int)concurrency);

  return startMetadataCompactionAppForShardedFileHelper<PARTY, 0>(
      inputFilePaths,
      outputGlobalParamsPaths,
      outputSecretSharesPaths,
      0,
      numThreads,
      numThreads,
      serverIp,
      port,
      numConversionsPerUser,
      computePublisherBreakdowns,
      epoch,
      useXorEncryption,
      tlsInfo);
}

} // namespace private_lift
