/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <future>
#include <memory>

#include "fbpcf/engine/communication/SocketPartyCommunicationAgentFactory.h"
#include "fbpcs/emp_games/pcf2_aggregation/AggregationApp.h"

namespace pcf2_aggregation {

inline std::vector<std::string> getIOInputFilenames(
    int32_t numFiles,
    std::string inputBasePath,
    int32_t fileStartIndex,
    bool use_postfix) {
  // get all input files (we have multiple files if they were sharded)
  std::vector<std::string> inputFilePaths;

  try {
    // if multiple files used (sharding)
    if (use_postfix) {
      for (auto i = 0; i < numFiles; i++) {
        std::string inputFilePath =
            folly::sformat("{}_{}", inputBasePath, (fileStartIndex + i));
        inputFilePaths.push_back(inputFilePath);
      }
    } else {
      inputFilePaths.push_back(inputBasePath);
    }

  } catch (const std::exception& e) {
    XLOG(ERR) << "Error: Exception caught in Aggregation run.\n \t error msg: "
              << e.what() << "\n \t input directory: " << inputBasePath;
    std::exit(1);
  }
  return inputFilePaths;
}

template <int PARTY, int index>
inline common::SchedulerStatistics startAggregationAppsForShardedFilesHelper(
    common::InputEncryption inputEncryption,
    common::Visibility outputVisibility,
    int startFileIndex,
    int remainingThreads,
    int numThreads,
    std::string serverIp,
    int port,
    std::string aggregationFormats,
    std::vector<std::string>& inputSecretShareFilenames,
    std::vector<std::string>& inputClearTextFilenames,
    std::vector<std::string>& outputFilenames) {
  // aggregate scheduler statistics across apps
  common::SchedulerStatistics schedulerStatistics{
      0, 0, 0, 0, folly::dynamic::object()};

  // split files evenly across threads
  auto remainingFiles =
      static_cast<std::int64_t>(inputSecretShareFilenames.size()) -
      startFileIndex;
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

    auto communicationAgentFactory = std::make_unique<
        fbpcf::engine::communication::SocketPartyCommunicationAgentFactory>(
        PARTY,
        partyInfos,
        false,
        "",
        "aggregation_traffic_for_thread_" + std::to_string(index));

    // Each AggregationApp runs numFiles sequentially on a single thread
    // Publisher uses even schedulerId and partner uses odd schedulerId
    auto app = std::make_unique<
        pcf2_aggregation::AggregationApp<PARTY, 2 * index + PARTY>>(
        inputEncryption,
        outputVisibility,
        std::move(communicationAgentFactory),
        aggregationFormats,
        inputSecretShareFilenames,
        inputClearTextFilenames,
        outputFilenames,
        startFileIndex,
        numFiles,
        numThreads);

    auto future = std::async([&app]() {
      app->run();
      return app->getSchedulerStatistics();
    });

    if constexpr (index < kMaxConcurrency) {
      if (remainingThreads > 1) {
        auto remainingStats =
            startAggregationAppsForShardedFilesHelper<PARTY, index + 1>(
                inputEncryption,
                outputVisibility,
                startFileIndex + numFiles,
                remainingThreads - 1,
                numThreads,
                serverIp,
                port,
                aggregationFormats,
                inputSecretShareFilenames,
                inputClearTextFilenames,
                outputFilenames);
        schedulerStatistics.add(remainingStats);
      }
    }
    auto stats = future.get();
    schedulerStatistics.add(stats);
  }
  return schedulerStatistics;
}

template <int PARTY>
inline common::SchedulerStatistics startAggregationAppsForShardedFiles(
    common::InputEncryption inputEncryption,
    common::Visibility outputVisibility,
    std::vector<std::string>& inputSecretShareFilenames,
    std::vector<std::string>& inputClearTextFilenames,
    std::vector<std::string>& outputFilenames,
    int16_t concurrency,
    std::string serverIp,
    int port,
    std::string aggregationFormats) {
  // use only as many threads as the number of files
  auto numThreads =
      std::min((int)inputSecretShareFilenames.size(), (int)concurrency);

  return startAggregationAppsForShardedFilesHelper<PARTY, 0>(
      inputEncryption,
      outputVisibility,
      0,
      numThreads,
      numThreads,
      serverIp,
      port,
      aggregationFormats,
      inputSecretShareFilenames,
      inputClearTextFilenames,
      outputFilenames);
}

} // namespace pcf2_aggregation
