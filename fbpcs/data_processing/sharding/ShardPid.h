/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <string>

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
    const std::string& hmacBase64Key);
} // namespace data_processing::sharder
