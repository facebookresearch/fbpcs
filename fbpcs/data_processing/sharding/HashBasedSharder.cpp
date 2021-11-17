/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "fbpcs/data_processing/sharding/HashBasedSharder.h"

#include <arpa/inet.h>

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
#include "fbpcs/data_processing/hash_slinging_salter/HashSlingingSalter.hpp"

namespace data_processing::sharder {
namespace detail {
std::vector<uint8_t> toBytes(const std::string& key) {
  std::vector<uint8_t> res(key.begin(), key.end());
  return res;
}

int32_t bytesToInt(const std::vector<uint8_t>& bytes) {
  int32_t res = 0;

  auto bytesInSizeT = sizeof(res) / sizeof(uint8_t);
  auto end = bytes.begin() + std::min(bytesInSizeT, bytes.size());
  std::copy(bytes.begin(), end, reinterpret_cast<uint8_t*>(&res));

  // Because we could be in a bizarre scenario where the publisher machine's
  // endianness differs from the partner machine's endianness, we rearrange the
  // bytes now to ensure a consistent representation. We assume the previously
  // copied bytes are in "network byte order" and convert them to a host long.
  return ntohl(res);
}

std::size_t getShardFor(const std::string& id, std::size_t numShards) {
  auto toInt = bytesToInt(toBytes(id));
  return toInt % numShards;
}
} // namespace detail

void HashBasedSharder::shard() const {
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
  for (const auto& tmpFile : tmpFiles) {
    *tmpFile << line << "\n";
  }
  XLOG(INFO) << "Got header line: '" << line;

  // Read lines and send to appropriate outFile repeatedly
  uint64_t lineIdx = 0;
  if (hmacKey_.empty()) {
    while (getline(inStream, line)) {
      detail::stripQuotes(line);
      auto commaPos = line.find_first_of(",");
      auto id = line.substr(0, commaPos);
      // Assumption: the string is *already* an HMAC hashed value
      // If hmacBase64Key is empty, the hashing already happened upstream.
      // This means we can reinterpret the id as a base64-encoded string.
      auto shard = detail::getShardFor(id, numShards);
      *tmpFiles.at(shard) << line << "\n";
      ++lineIdx;
      if (lineIdx % getLogRate() == 0) {
        XLOG(INFO) << "Processed line "
                   << private_lift::logging::formatNumber(lineIdx);
      }
    }
  } else {
    while (getline(inStream, line)) {
      detail::stripQuotes(line);
      auto commaPos = line.find_first_of(",");
      auto id = line.substr(0, commaPos);
      auto base64SaltedId =
          private_lift::hash_slinging_salter::base64SaltedHashFromBase64Key(
              id, hmacKey_);
      auto shard = detail::getShardFor(base64SaltedId, numShards);
      *tmpFiles.at(shard) << base64SaltedId << line.substr(commaPos) << "\n";
      ++lineIdx;
      if (lineIdx % getLogRate() == 0) {
        XLOG(INFO) << "Processed line "
                   << private_lift::logging::formatNumber(lineIdx);
      }
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
} // namespace data_processing::sharder
