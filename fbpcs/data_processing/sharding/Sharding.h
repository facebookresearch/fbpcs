/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <memory>
#include <string>
#include "fbpcf/engine/communication/IPartyCommunicationAgent.h"

namespace data_processing::sharder {

void runShard(
    const std::string& inputFilename,
    const std::string& outputFilenames,
    const std::string& outputBasePath,
    int32_t fileStartIndex,
    int32_t numOutputFiles,
    int32_t logEveryN);

void runShardPid(
    const std::string& inputFilename,
    const std::string& outputFilenames,
    const std::string& outputBasePath,
    int32_t fileStartIndex,
    int32_t numOutputFiles,
    int32_t logEveryN,
    const std::string& hmacBase64Key);

void runSecureRandomShard(
    const std::string& inputFilename,
    const std::string& outputFilenames,
    const std::string& outputBasePath,
    int32_t fileStartIndex,
    int32_t numOutputFiles,
    int32_t logEveryN,
    bool amISendingFirst,
    std::unique_ptr<fbpcf::engine::communication::IPartyCommunicationAgent>
        agent);

} // namespace data_processing::sharder
