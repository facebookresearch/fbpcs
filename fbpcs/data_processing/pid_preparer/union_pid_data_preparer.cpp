/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <filesystem>

#include <gflags/gflags.h>

#include "folly/init/Init.h"

// TODO: Rewrite for OSS?
#include "fbpcf/aws/AwsSdk.h"

#include "UnionPIDDataPreparer.h"

DEFINE_string(input_path, "", "Path to input CSV (with header)");
DEFINE_string(output_path, "", "Path where list of IDs should be output");
DEFINE_string(
    tmp_directory,
    "/tmp/",
    "Directory where temporary files should be saved before final write");
DEFINE_int32(max_column_cnt, 1, "Number of columns to write");
DEFINE_int32(log_every_n, 1'000'000, "How frequently to log updates");

int main(int argc, char** argv) {
  folly::init(&argc, &argv);
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  fbpcf::AwsSdk::aquire();

  std::filesystem::path tmpDirectory{FLAGS_tmp_directory};
  measurement::pid::UnionPIDDataPreparer preparer{
      FLAGS_input_path,
      FLAGS_output_path,
      tmpDirectory,
      FLAGS_max_column_cnt,
      FLAGS_log_every_n};

  preparer.prepare();
  return 0;
}
