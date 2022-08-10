/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "AttributionIdSpineFileCombiner.h"

#include <folly/String.h>
#include <folly/logging/xlog.h>

#include "fbpcs/data_processing/attribution_id_combiner/AttributionIdSpineCombinerOptions.h"
#include "fbpcs/data_processing/attribution_id_combiner/MrPidAttributionIdCombiner.h"
#include "fbpcs/data_processing/attribution_id_combiner/PidAttributionIdCombiner.h"
namespace pid::combiner {
const std::string PROTOCOL_PID = "PID";
const std::string PROTOCOL_MRPID = "MR_PID";

void attributionIdSpineFileCombiner() {
  XLOG(INFO) << "Started.";
  executeStrategy(FLAGS_protocol_type);
  XLOG(INFO) << "Finished.";
}

void executeStrategy(std::string protocol) {
  if (protocol == PROTOCOL_PID) {
    PidAttributionIdCombiner p;
    p.run();
  } else if (protocol == PROTOCOL_MRPID) {
    MrPidAttributionIdCombiner p;
    p.run();
  } else {
    XLOG(FATAL) << "Invalid FLAGS_protocol_type '" << FLAGS_protocol_type
                << "'. Expected 'PID' or 'MR_PID'.";
  }
}
} // namespace pid::combiner
