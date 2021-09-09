/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <algorithm>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <memory>
#include <sstream>
#include <string>
#include <vector>
#include "../hash_slinging_salter/HashSlingingSalter.hpp"

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
DEFINE_int32(hashing_prime, 37, "Prime number to assist in consistent hashing");
DEFINE_string(
    hmac_base64_key,
    "",
    "key to be used in optional hash salting step");

/* Utility to hash a string to an unsigned machine size integer.
 * Unsigned is important so overflow is properly defined.
 * Adapted from
 * https://stackoverflow.com/questions/8567238/hash-function-in-c-for-string-to-int
 */
std::size_t hashString(const std::string& s, uint64_t hashing_prime) {
  std::size_t res = 0;
  for (auto i = 0; i < s.length(); ++i) {
    res = hashing_prime * res + s[i];
  }
  return res;
}

void stripQuotes(std::string& s) {
  s.erase(std::remove(s.begin(), s.end(), '"'), s.end());
}

void shardFile(
    const std::string& inputFilename,
    const std::filesystem::path& tmpDirectory,
    const std::vector<std::string>& outputFilepaths,
    int32_t logEveryN,
    int32_t hashingPrime,
    const std::string& hmacBase64Key) {
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

  for (auto i = 0; i < numShards; ++i) {
    std::stringstream tmpName;
    tmpName << randomId << "_" << stem << "_" << i << extension;

    auto tmpFilepath = tmpDirectory / tmpName.str();

    tmpFilenames.push_back(tmpFilepath.string());
    tmpFiles.push_back(std::make_unique<std::ofstream>(tmpFilepath));
  }

  // First get the header and put it in all the output files
  std::string line;
  getline(inStream, line);
  stripQuotes(line);
  for (const auto& tmpFile : tmpFiles) {
    *tmpFile << line << "\n";
  }
  XLOG(INFO) << "Got header line: '" << line;

  // Read lines and send to appropriate outFile repeatedly
  uint64_t line_idx = 0;
  if (hmacBase64Key.empty()) {
    while (getline(inStream, line)) {
      stripQuotes(line);
      auto commaPos = line.find_first_of(",");
      auto id = line.substr(0, commaPos);
      auto shard = hashString(id, hashingPrime) % numShards;
      *tmpFiles.at(shard) << line << "\n";
      ++line_idx;
      if (line_idx % logEveryN == 0) {
        XLOG(INFO) << "Processed line "
                   << private_lift::logging::formatNumber(line_idx);
      }
    }
  } else {
    while (getline(inStream, line)) {
      stripQuotes(line);
      auto commaPos = line.find_first_of(",");
      auto id = line.substr(0, commaPos);
      auto base64SaltedId =
          private_lift::hash_slinging_salter::base64SaltedHashFromBase64Key(
              id, hmacBase64Key);
      auto shard = hashString(base64SaltedId, hashingPrime) % numShards;
      *tmpFiles.at(shard) << base64SaltedId << line.substr(commaPos) << "\n";
      ++line_idx;
      if (line_idx % logEveryN == 0) {
        XLOG(INFO) << "Processed line "
                   << private_lift::logging::formatNumber(line_idx);
      }
    }
  }

  XLOG(INFO) << "Finished after processing "
             << private_lift::logging::formatNumber(line_idx) << " lines.";

  XLOG(INFO) << "Now copying files to final output path...";
  for (auto i = 0; i < numShards; ++i) {
    auto outputDst = outputFilepaths.at(i);
    auto tmpFileSrc = tmpFilenames.at(i);

    if (outputDst == tmpFileSrc) {
      continue;
    }

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
      FLAGS_input_filename,
      tmpDirectory,
      outputFilepaths,
      FLAGS_log_every_n,
      FLAGS_hashing_prime,
      FLAGS_hmac_base64_key);

  return 0;
}
