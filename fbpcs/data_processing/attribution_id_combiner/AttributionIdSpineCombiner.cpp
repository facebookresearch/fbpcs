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
#include <fbpcf/io/FileManagerUtil.h>
#include <fbpcf/io/IInputStream.h>
#include <folly/Format.h>
#include <folly/Random.h>
#include <folly/init/Init.h>
#include <folly/logging/xlog.h>

#include <fbpcs/performance_tools/CostEstimation.h>
#include "fbpcf/io/api/BufferedReader.h"
#include "fbpcf/io/api/FileReader.h"
#include "fbpcs/data_processing/attribution_id_combiner/AttributionIdSpineCombinerOptions.h"
#include "fbpcs/data_processing/attribution_id_combiner/AttributionIdSpineCombinerUtil.h"
#include "fbpcs/data_processing/attribution_id_combiner/AttributionIdSpineFileCombiner.h"
#include "fbpcs/data_processing/common/FilepathHelpers.h"
#include "fbpcs/data_processing/common/S3CopyFromLocalUtil.h"

int main(int argc, char** argv) {
  folly::init(&argc, &argv);
  gflags::ParseCommandLineFlags(&argc, &argv, true);

  fbpcs::performance_tools::CostEstimation cost{
      "data_processing", FLAGS_log_cost_s3_bucket, FLAGS_log_cost_s3_region};
  cost.start();

  fbpcf::AwsSdk::aquire();

  signal(SIGPIPE, SIG_IGN);

  std::filesystem::path outputPath{FLAGS_output_path};
  std::filesystem::path tmpDirectory{FLAGS_tmp_directory};

  XLOG(INFO) << "Starting data_processing run on: data_path:" << FLAGS_data_path
             << ", spine_path: " << FLAGS_spine_path
             << ", output_path: " << FLAGS_output_path
             << ", tmp_directory: " << FLAGS_tmp_directory
             << ", sorting_strategy: " << FLAGS_sort_strategy
             << ", max_id_column_cnt: " << FLAGS_max_id_column_cnt;

  auto dataReader = std::make_unique<fbpcf::io::FileReader>(FLAGS_data_path);
  auto spineReader = std::make_unique<fbpcf::io::FileReader>(FLAGS_spine_path);
  auto bufferedDataReader = std::make_shared<fbpcf::io::BufferedReader>(
      std::move(dataReader),
      measurement::private_attribution::kBufferedReaderChunkSize);
  auto bufferedSpineReader = std::make_shared<fbpcf::io::BufferedReader>(
      std::move(spineReader),
      measurement::private_attribution::kBufferedReaderChunkSize);

  // Get a random ID to avoid potential name collisions if multiple
  // runs at the same time point to the same input file
  auto randomId = std::to_string(folly::Random::secureRand64());
  std::string tmpFilename = randomId + "_" +
      private_lift::filepath_helpers::getBaseFilename(outputPath);
  std::filesystem::path tmpFilepath = (tmpDirectory / tmpFilename);
  XLOG(INFO) << "Writing temporary file to " << tmpFilepath;
  std::ofstream tmpFile{tmpFilepath};

  pid::combiner::attributionIdSpineFileCombiner(
      bufferedDataReader, bufferedSpineReader, tmpFile, FLAGS_spine_path);
  tmpFile.close();
  bufferedDataReader->close();
  bufferedSpineReader->close();

  auto outputType = fbpcf::io::getFileType(outputPath);
  if (outputPath != tmpFilepath) {
    if (outputType == fbpcf::io::FileType::S3) {
      private_lift::s3_utils::uploadToS3(tmpFilepath, outputPath);
    } else if (outputType == fbpcf::io::FileType::Local) {
      std::filesystem::create_directories(outputPath.parent_path());
      std::filesystem::copy(
          tmpFilepath,
          outputPath,
          std::filesystem::copy_options::overwrite_existing);
    } else {
      throw std::runtime_error{"Unsupported output destination"};
    }

    // We need to make sure we clean up the tmpfiles now
    std::remove(tmpFilepath.c_str());
  }

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
