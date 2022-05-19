/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <gflags/gflags.h>
#include <future>
#include <memory>

#include "fbpcf/engine/communication/SocketPartyCommunicationAgentFactory.h"
#include "fbpcs/emp_games/lift/pcf2_calculator/CalculatorApp.h"

namespace private_lift {

inline std::pair<std::vector<std::string>, std::vector<std::string>>
getIOFilepaths(
    std::string inputBasePath,
    std::string outputBasePath,
    std::string inputDirectory,
    std::string outputDirectory,
    std::string inputFilenames,
    std::string outputFilenames,
    int32_t numFiles,
    int32_t fileStartIndex) {
  std::vector<std::string> inputFilepaths;
  std::vector<std::string> outputFilepaths;

  if (!inputBasePath.empty()) {
    std::string inputBasePathPrefix = inputBasePath + "_";
    std::string outputBasePathPrefix = outputBasePath + "_";
    for (auto i = fileStartIndex; i < fileStartIndex + numFiles; ++i) {
      inputFilepaths.push_back(inputBasePathPrefix + std::to_string(i));
      outputFilepaths.push_back(outputBasePathPrefix + std::to_string(i));
    }
  } else {
    std::filesystem::path inputDir{inputDirectory};
    std::filesystem::path outputDir{outputDirectory};

    std::vector<std::string> inputFilenamesVector;
    folly::split(',', inputFilenames, inputFilenamesVector);

    std::vector<std::string> outputFilenamesVector;
    folly::split(",", outputFilenames, outputFilenamesVector);

    // Make sure the number of input files equals output files
    CHECK_EQ(inputFilenamesVector.size(), outputFilenamesVector.size())
        << "Error: input_filenames and output_filenames have unequal sizes";

    for (std::size_t i = 0; i < inputFilenamesVector.size(); ++i) {
      inputFilepaths.push_back(inputDir / inputFilenamesVector[i]);
      outputFilepaths.push_back(outputDir / outputFilenamesVector[i]);
    }
  }
  return std::make_pair(inputFilepaths, outputFilepaths);
}

template <int PARTY, int index>
inline common::SchedulerStatistics startCalculatorAppsForShardedFilesHelper(
    int startFileIndex,
    int remainingThreads,
    int numThreads,
    std::string serverIp,
    int port,
    std::vector<std::string>& inputFilepaths,
    std::vector<std::string>& outputFilepaths,
    int numConversionsPerUser,
    int epoch) {
  // aggregate scheduler statistics across apps
  common::SchedulerStatistics schedulerStatistics{0, 0, 0, 0};

  // split files evenly across threads
  auto remainingFiles = inputFilepaths.size() - startFileIndex;
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
        PARTY, partyInfos, false, "");

    // Each CalculatorApp runs numFiles sequentially on a single thread
    // Publisher uses even schedulerId and partner uses odd schedulerId
    auto app = std::make_unique<CalculatorApp<2 * index + PARTY>>(
        PARTY,
        std::move(communicationAgentFactory),
        numConversionsPerUser,
        epoch,
        inputFilepaths,
        outputFilepaths,
        startFileIndex,
        numFiles);

    auto future = std::async([&app]() {
      app->run();
      return app->getSchedulerStatistics();
    });

    // We construct a CalculatorApp for each thread recursively because each app
    // has a different schedulerId, which is a template parameter
    if constexpr (index < kMaxConcurrency) {
      if (remainingThreads > 1) {
        auto remainingStats =
            startCalculatorAppsForShardedFilesHelper<PARTY, index + 1>(
                startFileIndex + numFiles,
                remainingThreads - 1,
                numThreads,
                serverIp,
                port,
                inputFilepaths,
                outputFilepaths,
                numConversionsPerUser,
                epoch);
        schedulerStatistics.add(remainingStats);
      }
    }
    auto stats = future.get();
    schedulerStatistics.add(stats);
  }
  return schedulerStatistics;
}

template <int PARTY>
inline common::SchedulerStatistics startCalculatorAppsForShardedFiles(
    std::vector<std::string>& inputFilepaths,
    std::vector<std::string>& outputFilepaths,
    int16_t concurrency,
    std::string serverIp,
    int port,
    int numConversionsPerUser,
    int epoch) {
  // use only as many threads as the number of files
  auto numThreads = std::min((int)inputFilepaths.size(), (int)concurrency);

  return startCalculatorAppsForShardedFilesHelper<PARTY, 0>(
      0,
      numThreads,
      numThreads,
      serverIp,
      port,
      inputFilepaths,
      outputFilepaths,
      numConversionsPerUser,
      epoch);
}

} // namespace private_lift
