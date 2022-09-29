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
#include "fbpcs/data_processing/common/FilepathHelpers.h"
#include "fbpcs/data_processing/private_id_dfca_id_combiner/PrivateIdDfcaIdSpineCombinerOptions.h"
#include "fbpcs/data_processing/private_id_dfca_id_combiner/PrivateIdDfcaIdSpineCombinerUtil.h"
#include "fbpcs/data_processing/private_id_dfca_id_combiner/PrivateIdDfcaIdSpineFileCombiner.h"

int main(int argc, char** argv) {
  folly::init(&argc, &argv);
  gflags::ParseCommandLineFlags(&argc, &argv, true);

  fbpcs::performance_tools::CostEstimation cost{
      "data_processing", FLAGS_log_cost_s3_bucket, FLAGS_log_cost_s3_region};
  cost.start();

  fbpcf::AwsSdk::aquire();

  signal(SIGPIPE, SIG_IGN);

  pid::combiner::privateIdDfcaIdSpineFileCombiner();

  cost.end();
  XLOG(INFO) << cost.getEstimatedCostString();

  if (FLAGS_log_cost) {
    auto run_name = (FLAGS_run_name != "") ? FLAGS_run_name : "temp_run_name";
    folly::dynamic extra_info = folly::dynamic::object(
        "spine_path", FLAGS_spine_path)("data_path", FLAGS_data_path)(
        "output_path",
        FLAGS_output_path)("sort_strategy", FLAGS_sort_strategy)("run_id", FLAGS_run_id);

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
