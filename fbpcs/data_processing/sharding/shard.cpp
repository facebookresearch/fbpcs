/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <string>
#include <vector>

#include <gflags/gflags.h>

#include <fbpcf/aws/AwsSdk.h>
#include <folly/init/Init.h>
#include <folly/logging/xlog.h>

#include "fbpcs/data_processing/common/FilepathHelpers.h"
#include "fbpcs/data_processing/common/Logging.h"
#include "fbpcs/data_processing/common/S3CopyFromLocalUtil.h"
#include "fbpcs/data_processing/sharding/RoundRobinBasedSharder.h"

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
    "Directory where temporary files should be saved before final write");
DEFINE_int32(log_every_n, 1000000, "How frequently to log updates");

using namespace data_processing::sharder;

int main(int argc, char** argv) {
  folly::init(&argc, &argv);
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  fbpcf::AwsSdk::aquire();

  if (!FLAGS_output_filenames.empty()) {
    std::stringstream ss{FLAGS_output_filenames};
    std::vector<std::string> outputFilepaths;
    while (ss.good()) {
      std::string substr;
      getline(ss, substr, ',');
      outputFilepaths.push_back(std::move(substr));
    }
    RoundRobinBasedSharder sharder{
        FLAGS_input_filename, outputFilepaths, FLAGS_log_every_n};
    sharder.shard();
  } else if (!FLAGS_output_base_path.empty() && FLAGS_num_output_files > 0) {
    std::size_t startIndex = FLAGS_file_start_index;
    std::size_t endIndex = startIndex + FLAGS_num_output_files;
    RoundRobinBasedSharder sharder{
        FLAGS_input_filename,
        FLAGS_output_base_path,
        startIndex,
        endIndex,
        FLAGS_log_every_n};
    sharder.shard();
  } else {
    XLOG(ERR) << "Error: specify --output_filenames or --output_base_path, "
                 "--file_start_index, and --num_output_files";
    std::exit(1);
  }

  return 0;
}
