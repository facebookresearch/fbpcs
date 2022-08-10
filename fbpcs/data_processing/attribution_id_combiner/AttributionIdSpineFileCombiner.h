/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <filesystem>
#include <unordered_map>
#include <vector>
#include "fbpcf/io/api/BufferedReader.h"
#include "fbpcf/io/api/FileReader.h"

namespace pid::combiner {
extern const std::string PROTOCOL_PID;
extern const std::string PROTOCOL_MRPID;
/**
 * attributionIdSpineFileCombiner() will run executeStrategy() according to
 *FLAGS_protocol_type. If protocol type is PID, run PidAttributionIdComibiner
 *and get the file for the compute stage.
 *
 **/
void attributionIdSpineFileCombiner();
/**
 * executeStrategy() executes differernt strategy according to different
 *protocol type
 * @param protocol protocol type is PID or MR_PID
 **/
void executeStrategy(std::string protocol);
} // namespace pid::combiner
