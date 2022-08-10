/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <filesystem>
#include <fstream>
#include <string>

#include <gflags/gflags.h>
#include <signal.h>

#include <fbpcf/aws/AwsSdk.h>
#include <folly/Format.h>
#include <folly/Random.h>
#include <folly/init/Init.h>
#include <folly/logging/xlog.h>

#include <fbpcs/performance_tools/CostEstimation.h>
#include "fbpcf/io/api/BufferedReader.h"
#include "fbpcf/io/api/FileIOWrappers.h"
#include "fbpcf/io/api/FileReader.h"
#include "fbpcs/data_processing/attribution_id_combiner/AttributionIdSpineCombinerOptions.h"
#include "fbpcs/data_processing/attribution_id_combiner/AttributionIdSpineCombinerUtil.h"
#include "fbpcs/data_processing/attribution_id_combiner/AttributionIdSpineFileCombiner.h"
#include "fbpcs/data_processing/common/FilepathHelpers.h"

int main(int argc, char** argv) {
  folly::init(&argc, &argv);
  gflags::ParseCommandLineFlags(&argc, &argv, true);

  fbpcs::performance_tools::CostEstimation cost{
      "data_processing", FLAGS_log_cost_s3_bucket, FLAGS_log_cost_s3_region};
  cost.start();

  fbpcf::AwsSdk::aquire();

  signal(SIGPIPE, SIG_IGN);

  pid::combiner::attributionIdSpineFileCombiner();

  cost.end();
  XLOG(INFO) << cost.getEstimatedCostString();

  if (FLAGS_log_cost) {
    auto run_name = (FLAGS_run_name != "") ? FLAGS_run_name : "temp_run_name";
    folly::dynamic extra_info = folly::dynamic::object(
        "padding_size", FLAGS_padding_size)("spine_path", FLAGS_spine_path)(
        "data_path",
        FLAGS_data_path)("output_path", FLAGS_output_path)("sort_strategy", FLAGS_sort_strategy);

    XLOGF(
        INFO,
        "{}",
        cost.writeToS3(
            "",
            run_name,
            cost.getEstimatedCostDynamic(run_name, "", extra_info)));
  }

  return 0;
}
