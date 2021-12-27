/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <gflags/gflags.h>
#include <string>
#include <utility>
#include <vector>

#include <fbpcf/mpc/EmpGame.h>
#include "folly/init/Init.h"
#include "folly/logging/xlog.h"

#include <fbpcf/aws/AwsSdk.h>
#include <fbpcf/mpc/MpcAppExecutor.h>
#include "AttributionApp.h"

namespace measurement::private_attribution {

template <int PARTY, fbpcf::Visibility OUTPUT_VISIBILITY>
inline void startAttributionAppsForShardedFiles(
    std::vector<std::string> inputFilenames,
    std::vector<std::string> outputFilenames,
    int16_t concurrency,
    std::string serverIp,
    int16_t port,
    std::string attributionRules,
    std::string aggregators) {
  std::vector<std::unique_ptr<measurement::private_attribution::
                                  AttributionApp<PARTY, OUTPUT_VISIBILITY>>>
      attributionApps;
  for (std::vector<std::string>::size_type i = 0; i < inputFilenames.size();
       i++) {
    attributionApps.push_back(
        std::make_unique<measurement::private_attribution::
                             AttributionApp<PARTY, OUTPUT_VISIBILITY>>(
            serverIp,
            port + i,
            attributionRules,
            aggregators,
            inputFilenames.at(i),
            outputFilenames.at(i)));
  }

  // executor attributionApps using fbpcf::MpcAppExecutor
  fbpcf::MpcAppExecutor<measurement::private_attribution::
                            AttributionApp<PARTY, OUTPUT_VISIBILITY>>
      executor{concurrency};
  executor.execute(attributionApps);
}

inline std::pair<std::vector<std::string>, std::vector<std::string>>
getIOFilenames(
    int32_t numFiles,
    std::string inputBasePath,
    std::string outputBasePath,
    int32_t fileStartIndex) {
  // get all input files (we have multiple files if they were sharded)
  std::vector<std::string> inputFilenames;
  std::vector<std::string> outputFilenames;

  try {
    for (int32_t i = 0; i < numFiles; i++) {
      std::string inputPathName =
          folly::sformat("{}_{}", inputBasePath, (fileStartIndex + i));
      std::string outputPathName =
          folly::sformat("{}_{}", outputBasePath, (fileStartIndex + i));
      inputFilenames.push_back(inputPathName);
      outputFilenames.push_back(outputPathName);
    }
  } catch (const std::exception& e) {
    XLOG(ERR) << "Error: Exception caught in Attribution run.\n \t error msg: "
              << e.what() << "\n \t input directory: " << inputBasePath;
    std::exit(1);
  }
  return std::make_pair(inputFilenames, outputFilenames);
}

inline std::string exec(const char* cmd) {
  std::array<char, 128> buffer;
  std::string result;
  std::unique_ptr<FILE, decltype(&pclose)> pipe(popen(cmd, "r"), pclose);
  if (!pipe) {
    throw std::runtime_error("popen() failed!");
  }
  while (fgets(buffer.data(), buffer.size(), pipe.get()) != nullptr) {
    result += buffer.data();
  }
  return result;
}

} // namespace measurement::private_attribution
