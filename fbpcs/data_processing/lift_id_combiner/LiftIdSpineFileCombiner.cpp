/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "LiftIdSpineFileCombiner.h"

#include <folly/String.h>
#include <folly/logging/xlog.h>

// TODO: Rewrite for OSS?
#include "fbpcs/data_processing/lift_id_combiner/MrPidLiftIdCombiner.h"
#include "fbpcs/data_processing/lift_id_combiner/PidLiftIdCombiner.h"

namespace pid::combiner {
const std::string PROTOCOL_PID = "PID";
const std::string PROTOCOL_MRPID = "MR_PID";
void combineFile(
    std::string dataPath,
    std::string spineIdFilePath,
    std::string outputStr,
    std::string tmpDirectory,
    std::string sortStrategy,
    int maxIdColumnCnt,
    std::string protocolType) {
  XLOG(INFO) << "Started.";
  executeStrategy(
      dataPath,
      spineIdFilePath,
      outputStr,
      tmpDirectory,
      sortStrategy,
      maxIdColumnCnt,
      protocolType);
  XLOG(INFO) << "Finished.";
}

void executeStrategy(
    std::string dataPath,
    std::string spineIdFilePath,
    std::string outputStr,
    std::string tmpDirectory,
    std::string sortStrategy,
    int maxIdColumnCnt,
    std::string protocolType) {
  if (protocolType == "PID") {
    PidLiftIdCombiner p(
        dataPath,
        spineIdFilePath,
        outputStr,
        tmpDirectory,
        sortStrategy,
        maxIdColumnCnt,
        protocolType);
    p.run();
  } else if (protocolType == PROTOCOL_MRPID) {
    MrPidLiftIdCombiner p(
        spineIdFilePath,
        outputStr,
        tmpDirectory,
        sortStrategy,
        maxIdColumnCnt,
        protocolType);
    p.run();
  } else {
    XLOG(FATAL) << "Invalid FLAGS_protocol_type '" << FLAGS_protocol_type
                << "'. Expected 'PID' or 'MR_PID'.";
  }
}
} // namespace pid::combiner
