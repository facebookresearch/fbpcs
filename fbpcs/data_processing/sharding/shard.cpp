/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <cstdio>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <memory>
#include <sstream>
#include <string>
#include <vector>

#include <gflags/gflags.h>

#include "folly/Random.h"
#include "folly/init/Init.h"
#include "folly/logging/xlog.h"
// TODO: Rewrite for open source?
#include "fbpcf/aws/AwsSdk.h"
#include "fbpcf/io/FileManagerUtil.h"

// TODO: Rewrite for open source?
#include "../common/FilepathHelpers.h"
#include "../common/Logging.h"
#include "../common/S3CopyFromLocalUtil.h"

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

void shardFile(
    const std::string& inputFilename,
    const std::filesystem::path& tmpDirectory,
    const std::vector<std::string>& outputFilepaths,
    int32_t logEveryN) {
  auto numShards = outputFilepaths.size();
  auto inStreamPtr = fbpcf::io::getInputStream(inputFilename);
  auto& inStream = inStreamPtr->get();

  std::vector<std::string> tmpFilenames;
  std::vector<std::unique_ptr<std::ofstream>> tmpFiles;

  auto filename = std::filesystem::path{
      private_lift::filepath_helpers::getBaseFilename(inputFilename)};
  auto stem = filename.stem().string();
  auto extension = filename.extension().string();
  // Get a random ID to avoid potential name collisions if multiple
  // runs at the same time point to the same input file
  auto randomId = std::to_string(folly::Random::secureRand64());

  for (auto i = 0; i < outputFilepaths.size(); ++i) {
    std::stringstream tmpName;
    tmpName << randomId << "_" << stem << "_" << i << extension;

    auto tmpFilepath = tmpDirectory / tmpName.str();

    tmpFilenames.push_back(tmpFilepath.string());
    tmpFiles.push_back(std::make_unique<std::ofstream>(tmpFilepath));
  }

  // First get the header and put it in all the tmp files
  std::string line;
  getline(inStream, line);
  XLOG(INFO) << "Got header line: " << line;
  for (const auto& tmpFile : tmpFiles) {
    *tmpFile << line << "\n";
  }

  // Read lines and send to appropriate tmpFile repeatedly
  uint64_t lineIdx = 0;
  while (getline(inStream, line)) {
    auto shard = lineIdx % numShards;
    *tmpFiles.at(shard) << line << "\n";
    ++lineIdx;
    if (lineIdx % logEveryN == 0) {
      XLOG(INFO) << "Processed line "
                 << private_lift::logging::formatNumber(lineIdx);
    }
  }

  XLOG(INFO) << "Finished after processing "
             << private_lift::logging::formatNumber(lineIdx) << " lines";

  XLOG(INFO) << "Now copying files to final output path...";
  for (auto i = 0; i < numShards; ++i) {
    auto outputDst = outputFilepaths.at(i);
    auto tmpFileSrc = tmpFilenames.at(i);
    // Reset underlying unique_ptr to ensure buffer gets flushed
    tmpFiles.at(i).reset();

    XLOG(INFO) << "Writing " << tmpFileSrc << " -> " << outputDst;
    auto outputType = fbpcf::io::getFileType(outputDst);
    if (outputType == fbpcf::io::FileType::S3) {
      private_lift::s3_utils::uploadToS3(tmpFileSrc, outputDst);
    } else if (outputType == fbpcf::io::FileType::Local) {
      std::filesystem::copy(
          tmpFileSrc,
          outputDst,
          std::filesystem::copy_options::overwrite_existing);
    } else {
      throw std::runtime_error{"Unsupported output destination"};
    }
    // We need to make sure we clean up the tmpfiles now
    std::remove(tmpFileSrc.c_str());
  }
  XLOG(INFO) << "All file writes successful";
}

int main(int argc, char** argv) {
  folly::init(&argc, &argv);
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  fbpcf::AwsSdk::aquire();

  std::filesystem::path tmpDirectory{FLAGS_tmp_directory};
  std::vector<std::string> outputFilepaths;

  if (!FLAGS_output_filenames.empty()) {
    std::stringstream ss{FLAGS_output_filenames};
    while (ss.good()) {
      std::string substr;
      getline(ss, substr, ',');
      outputFilepaths.push_back(std::move(substr));
    }
  } else if (!FLAGS_output_base_path.empty() && FLAGS_num_output_files > 0) {
    std::string output_base_path = FLAGS_output_base_path + "_";
    for (auto i = FLAGS_file_start_index;
         i < FLAGS_file_start_index + FLAGS_num_output_files;
         ++i) {
      outputFilepaths.push_back(output_base_path + std::to_string(i));
    }
  }

  if (outputFilepaths.empty()) {
    XLOG(ERR)
        << "Error: specify --output_filenames or --output_base_path, --file_start_index, and --num_output_files";
    std::exit(1);
  }

  shardFile(
      FLAGS_input_filename, tmpDirectory, outputFilepaths, FLAGS_log_every_n);

  return 0;
}
