/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <gflags/gflags.h>

#include <fbpcf/aws/AwsSdk.h>
#include <folly/init/Init.h>
#include <signal.h>

#include "fbpcs/data_processing/sharding/Sharding.h"

DEFINE_string(input_filename, "", "Name of the input file");
DEFINE_string(
    output_filenames,
    "",
    "Comma-separated list of file paths for output");
DEFINE_string(
    output_base_path,
    "",
    "Local or s3 base path where output files are written to");
DEFINE_int32(
    file_start_index,
    0,
    "First file that will be created from base path");
DEFINE_int32(num_output_files, 0, "Number of files that should be created");
DEFINE_string(
    tmp_directory,
    "/tmp/",
    "[Deprecated] Unused argument kept for historical purposes");
DEFINE_int32(log_every_n, 1000000, "How frequently to log updates");
DEFINE_string(
    hmac_base64_key,
    "",
    "key to be used in optional hash salting step");

int main(int argc, char** argv) {
  folly::init(&argc, &argv);
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  fbpcf::AwsSdk::aquire();

  signal(SIGPIPE, SIG_IGN);

  data_processing::sharder::runShardPid(
      FLAGS_input_filename,
      FLAGS_output_filenames,
      FLAGS_output_base_path,
      FLAGS_file_start_index,
      FLAGS_num_output_files,
      FLAGS_log_every_n,
      FLAGS_hmac_base64_key);
  return 0;
}
