/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <gflags/gflags.h>
#include <string>
#include <utility>
#include <vector>

#include <fbpcf/aws/AwsSdk.h>
#include <fbpcf/mpc/EmpGame.h>
#include <fbpcf/mpc/MpcAppExecutor.h>
#include "fbpcs/emp_games/attribution/decoupled_aggregation/AggregationApp.h"
#include "folly/init/Init.h"
#include "folly/logging/xlog.h"

namespace aggregation::private_aggregation {

inline std::vector<std::string> getIOInputFilenames(
    int32_t numFiles,
    std::string inputBasePath,
    int32_t fileStartIndex,
    bool use_postfix) {
  // private attribution supports multiple attribution output file creation.
  // thus including support for multiple input files in aggregation game.
  std::vector<std::string> inputFilePaths;

  try {
    // if multiple of attribution output files is greater than 1.
    if (use_postfix) {
      for (int32_t i = 0; i < numFiles; i++) {
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

template <int PARTY, fbpcf::Visibility OUTPUT_VISIBILITY>
inline void startPrivateAggregationApp(
    std::vector<std::string> inputSecretShareFilePaths,
    std::vector<std::string> inputClearTextFilePaths,
    std::vector<std::string> outputFilePaths,
    std::string serverIp,
    int16_t port,
    std::string aggregationFormat,
    int16_t concurrency) {
  XLOG(INFO) << "Calling private aggregation App";
  std::vector<std::unique_ptr<aggregation::private_aggregation::
                                  AggregationApp<PARTY, OUTPUT_VISIBILITY>>>
      aggregationApps;
  CHECK_EQ(inputSecretShareFilePaths.size(), inputClearTextFilePaths.size())
      << "number of attribution results and metadata files not matching.";
  for (std::vector<std::string>::size_type i = 0;
       i < inputSecretShareFilePaths.size();
       i++) {
    aggregationApps.push_back(
        std::make_unique<aggregation::private_aggregation::
                             AggregationApp<PARTY, OUTPUT_VISIBILITY>>(
            serverIp,
            port + i,
            aggregationFormat,
            inputSecretShareFilePaths.at(i),
            inputClearTextFilePaths.at(i),
            outputFilePaths.at(i)));
  }

  fbpcf::MpcAppExecutor<aggregation::private_aggregation::
                            AggregationApp<PARTY, OUTPUT_VISIBILITY>>
      executor{concurrency};
  executor.execute(aggregationApps);
}

} // namespace aggregation::private_aggregation
