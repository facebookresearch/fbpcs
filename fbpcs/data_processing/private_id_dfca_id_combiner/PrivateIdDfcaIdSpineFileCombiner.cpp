/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "fbpcs/data_processing/private_id_dfca_id_combiner/PrivateIdDfcaIdSpineFileCombiner.h"

#include <folly/String.h>
#include <folly/logging/xlog.h>

#include "fbpcs/data_processing/private_id_dfca_id_combiner/MrPidPrivateIdDfcaIdCombiner.h"
#include "fbpcs/data_processing/private_id_dfca_id_combiner/PidPrivateIdDfcaIdCombiner.h"
#include "fbpcs/data_processing/private_id_dfca_id_combiner/PrivateIdDfcaIdSpineCombinerOptions.h"
namespace pid::combiner {
const std::string PROTOCOL_PID = "PID";
const std::string PROTOCOL_MRPID = "MR_PID";

void privateIdDfcaIdSpineFileCombiner() {
  XLOG(INFO) << "Started.";
  executeStrategy(FLAGS_protocol_type);
  XLOG(INFO) << "Finished.";
}

void executeStrategy(std::string protocol) {
  if (protocol == PROTOCOL_PID) {
    PidPrivateIdDfcaIdCombiner p;
    p.run();
  } else if (protocol == PROTOCOL_MRPID) {
    MrPidPrivateIdDfcaIdCombiner p;
    p.run();
  } else {
    XLOG(FATAL) << "Invalid FLAGS_protocol_type '" << FLAGS_protocol_type
                << "'. Expected 'PID' or 'MR_PID'.";
  }
}
} // namespace pid::combiner
