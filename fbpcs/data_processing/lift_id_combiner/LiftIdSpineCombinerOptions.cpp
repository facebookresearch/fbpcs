/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <gflags/gflags.h>

#include "LiftIdSpineCombinerOptions.h"

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

DEFINE_int32(
    multi_conversion_limit,
    25,
    "How many conversions to retain per id");
DEFINE_string(
    sort_strategy,
    "sort",
    "Sorting strategy selected for the output data - options: (sort|keep_original)");
