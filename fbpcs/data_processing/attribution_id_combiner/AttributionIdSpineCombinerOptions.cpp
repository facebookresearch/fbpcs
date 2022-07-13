/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <gflags/gflags.h>

#include "AttributionIdSpineCombinerOptions.h"

DEFINE_int32(padding_size, 4, "Size of aggregated rows to retain");
DEFINE_string(spine_path, "", "File path which contains the identity spine");
DEFINE_string(data_path, "", "File path which contains the data file");
DEFINE_string(
    output_path,
    "",
    "File path with combined output from the identity spine");
DEFINE_string(
    tmp_directory,
    "/tmp/",
    "Directory where temporary files should be saved before final write");
DEFINE_string(run_name, "", "User given name used to write cost info in S3");
DEFINE_string(
    sort_strategy,
    "sort",
    "Sorting strategy selected for the output data - options: (sort|keep_original)");
DEFINE_bool(
    log_cost,
    false,
    "Log cost info into cloud which will be used for dashboard");
DEFINE_int32(max_id_column_cnt, 1, "Maximum number of id columns to use as id");
DEFINE_string(log_cost_s3_bucket, "cost-estimation-logs", "s3 bucket name");
DEFINE_string(
    log_cost_s3_region,
    ".s3.us-west-2.amazonaws.com/",
    "s3 region name");
