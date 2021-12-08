/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "fbpcs/data_processing/sharding/ShardPid.h"

#include <folly/String.h>
#include <folly/logging/xlog.h>

#include "fbpcs/data_processing/sharding/HashBasedSharder.h"

namespace data_processing::sharder {

/**
 * Create a separate function to allow for easy testing
 */
void runShardPid(
    const std::string& inputFilename,
    const std::string& outputFilenames,
    const std::string& outputBasePath,
    int32_t fileStartIndex,
    int32_t numOutputFiles,
    int32_t logEveryN,
    const std::string& hmacBase64Key) {
  if (!outputFilenames.empty()) {
    std::vector<std::string> outputFilepaths;
    folly::split(',', outputFilenames, outputFilepaths);
    HashBasedSharder sharder{
        inputFilename, outputFilepaths, logEveryN, hmacBase64Key};
    sharder.shard();
  } else if (!outputBasePath.empty() && numOutputFiles > 0) {
    std::size_t startIndex = fileStartIndex;
    std::size_t endIndex = startIndex + numOutputFiles;
    HashBasedSharder sharder{
        inputFilename,
        outputBasePath,
        startIndex,
        endIndex,
        logEveryN,
        hmacBase64Key};
    sharder.shard();
  } else {
    XLOG(FATAL) << "Error: specify --output_filenames or --output_base_path, "
                   "--file_start_index, and --num_output_files";
  }
}

} // namespace data_processing::sharder
