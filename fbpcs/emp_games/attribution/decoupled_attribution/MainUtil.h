/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <string>
#include <utility>
#include <vector>
#include <gflags/gflags.h>

#include "folly/init/Init.h"
#include "folly/logging/xlog.h"
#include <fbpcf/mpc/EmpGame.h>
#include <fbpcf/aws/AwsSdk.h>
#include <fbpcf/mpc/MpcAppExecutor.h>

#include "fbpcs/emp_games/attribution/decoupled_attribution/AttributionApp.h"


namespace aggregation::private_attribution {

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

  try{
    // if multiple files used (sharding)
    if (use_postfix){
      for (int32_t i = 0; i < numFiles; i++) {
        std::string inputPathName = folly::sformat(
            "{}_{}", inputBasePath, (fileStartIndex + i));
        std::string outputPathName = folly::sformat(
            "{}_{}", outputBasePath, (fileStartIndex + i));
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

template <int PARTY, fbpcf::Visibility OUTPUT_VISIBILITY>
inline void startAttributionAppsForShardedFiles(
  std::vector<std::string> inputFilenames,
  std::vector<std::string> outputFilenames,
  int16_t concurrency,
  std::string serverIp,
  int16_t port,
  std::string attributionRules) {
    std::vector<std::unique_ptr<aggregation::private_attribution::
                                  AttributionApp<PARTY, OUTPUT_VISIBILITY>>>
      attributionApps;
    for (std::vector<std::string>::size_type i = 0; i < inputFilenames.size(); i++) {
      attributionApps.push_back(
          std::make_unique<aggregation::private_attribution::
                              AttributionApp<PARTY, OUTPUT_VISIBILITY>>(
              serverIp,
              port + i,
              attributionRules,
              inputFilenames.at(i),
              outputFilenames.at(i)));
    }

    fbpcf::MpcAppExecutor<aggregation::private_attribution::
                        AttributionApp<PARTY, OUTPUT_VISIBILITY>>
        executor{concurrency};
    executor.execute(attributionApps);

}

} // namespace aggregation::private_attribution
