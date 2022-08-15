/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <filesystem>
#include <unordered_map>

#include "LiftIdSpineMultiConversionInput.h"

namespace pid::combiner {
extern const std::string PROTOCOL_PID;
extern const std::string PROTOCOL_MRPID;
/*
 * This chunk size has to be large enough that we don't make
 * unnecessary trips to cloud storage but small enough that
 * we don't cause OOM issues. This chunk size was chosen based
 * on the size of our containers as well as the expected size
 * of our files to fit the aforementioned constraints.
 */
constexpr size_t kBufferedReaderChunkSize = 1073741824; // 2^30
void combineFile(
    std::string dataPath,
    std::string spineIdFilePath,
    std::string outputStr,
    std::string tmpDirectory,
    std::string sortStrategy,
    int maxIdColumnCnt,
    std::string protocolType);
void executeStrategy(
    std::string dataPath,
    std::string spineIdFilePath,
    std::string outputStr,
    std::string tmpDirectory,
    std::string sortStrategy,
    int maxIdColumnCnt,
    std::string protocolType);
} // namespace pid::combiner
