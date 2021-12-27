/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "fbpcs/data_processing/sharding/GenericSharder.h"

#include <algorithm>
#include <filesystem>
#include <fstream>
#include <memory>
#include <sstream>
#include <stdexcept>
#include <string>
#include <vector>

#include <fbpcf/io/FileManagerUtil.h>
#include <folly/Random.h>
#include <folly/logging/xlog.h>

#include "fbpcs/data_processing/common/FilepathHelpers.h"
#include "fbpcs/data_processing/common/Logging.h"
#include "fbpcs/data_processing/common/S3CopyFromLocalUtil.h"

namespace data_processing::sharder {
namespace detail {
void stripQuotes(std::string& s) {
  s.erase(std::remove(s.begin(), s.end(), '"'), s.end());
}

void dos2Unix(std::string& s) {
  s.erase(std::remove(s.begin(), s.end(), '\r'), s.end());
}
} // namespace detail

std::vector<std::string> GenericSharder::genOutputPaths(
    const std::string& outputBasePath,
    std::size_t startIndex,
    std::size_t endIndex) {
  std::vector<std::string> res;
  for (std::size_t i = startIndex; i < endIndex; ++i) {
    res.push_back(outputBasePath + '_' + std::to_string(i));
  }
  return res;
}

void GenericSharder::shard() {
  std::size_t numShards = getOutputPaths().size();
  auto inStreamPtr = fbpcf::io::getInputStream(getInputPath());
  auto& inStream = inStreamPtr->get();

  std::filesystem::path tmpDirectory{"/tmp"};
  std::vector<std::string> tmpFilenames;
  std::vector<std::unique_ptr<std::ofstream>> tmpFiles;

  auto filename = std::filesystem::path{
      private_lift::filepath_helpers::getBaseFilename(getInputPath())};
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
  detail::stripQuotes(line);
  detail::dos2Unix(line);
  for (const auto& tmpFile : tmpFiles) {
    *tmpFile << line << "\n";
  }
  XLOG(INFO) << "Got header line: '" << line << "'";

  // Read lines and send to appropriate outFile repeatedly
  uint64_t lineIdx = 0;
  while (getline(inStream, line)) {
    detail::stripQuotes(line);
    detail::dos2Unix(line);
    shardLine(std::move(line), tmpFiles);
    ++lineIdx;
    if (lineIdx % getLogRate() == 0) {
      XLOG(INFO) << "Processed line "
                 << private_lift::logging::formatNumber(lineIdx);
    }
  }

  XLOG(INFO) << "Finished after processing "
             << private_lift::logging::formatNumber(lineIdx) << " lines.";

  XLOG(INFO) << "Now copying files to final output path...";
  for (auto i = 0; i < numShards; ++i) {
    auto outputDst = getOutputPaths().at(i);
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

void GenericSharder::shardLine(
    std::string line,
    const std::vector<std::unique_ptr<std::ofstream>>& outFiles) {
  auto commaPos = line.find_first_of(",");
  auto id = line.substr(0, commaPos);
  auto shard = getShardFor(id, outFiles.size());
  *outFiles.at(shard) << line << "\n";
}
} // namespace data_processing::sharder
