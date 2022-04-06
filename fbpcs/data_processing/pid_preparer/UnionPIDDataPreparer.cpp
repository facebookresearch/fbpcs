/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "UnionPIDDataPreparer.h"

#include <algorithm>
#include <cstdint>
#include <cstdlib>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <iterator>
#include <memory>
#include <sstream>
#include <string>
#include <unordered_set>
#include <vector>

#include <re2/re2.h>

#include "folly/Random.h"
#include "folly/logging/xlog.h"

// TODO: Rewrite for OSS?
#include "fbpcf/io/FileManagerUtil.h"

#include "../common/FilepathHelpers.h"
#include "../common/Logging.h"
#include "../common/S3CopyFromLocalUtil.h"

namespace measurement::pid {

static const std::string kIdColumnPrefix = "id_";

template <typename T>
static std::string vectorToString(const std::vector<T>& vec) {
  std::stringstream buf;
  buf << "[";
  bool first = true;
  for (const auto& col : vec) {
    if (!first) {
      buf << ", ";
    }
    buf << col;
    first = false;
  }
  buf << "]";
  return buf.str();
}

static std::vector<std::string> splitByCharDelimiter(
    std::string& str,
    const char& delim) {
  // Preprocessing step: Remove spaces if any
  str.erase(std::remove(str.begin(), str.end(), ' '), str.end());
  std::vector<std::string> tokens;
  std::stringstream ss(str);
  std::string token;
  while (std::getline(ss, token, delim)) {
    tokens.push_back(token);
  }
  return tokens;
}

std::vector<std::string> UnionPIDDataPreparer::split(
    std::string& str,
    const std::string& delim) const {
  // Preprocessing step: Remove spaces if any
  str.erase(std::remove(str.begin(), str.end(), ' '), str.end());
  std::vector<std::string> tokens;
  re2::RE2 rgx{delim};
  re2::StringPiece input{str}; // Wrap a StringPiece around it

  std::string token;
  while (RE2::Consume(&input, rgx, &token)) {
    tokens.push_back(token);
  }
  return tokens;
}

UnionPIDDataPreparerResults UnionPIDDataPreparer::prepare() const {
  UnionPIDDataPreparerResults res;
  auto inStreamPtr = fbpcf::io::getInputStream(inputPath_);
  auto& inStream = inStreamPtr->get();

  // Get a random ID to avoid potential name collisions if multiple
  // runs at the same time point to the same input file
  auto randomId = std::to_string(folly::Random::secureRand64());
  std::string tmpFilename = randomId + "_" +
      private_lift::filepath_helpers::getBaseFilename(inputPath_) + "_prepared";
  auto tmpFilepath = (tmpDirectory_ / tmpFilename).string();
  std::cout << "\t\tCreated temporary filepath --> " << tmpFilepath << '\n';
  auto tmpFile = std::make_unique<std::ofstream>(tmpFilename);

  std::string line;

  getline(inStream, line);
  auto header = splitByCharDelimiter(line, ',');
  // auto header = split(line, kCommaSplitRegex);
  auto idIter = header.begin();
  std::vector<std::int64_t> idColumnIndices;

  // find indices of columns with its column name start with kIdColumnPrefix
  while (
      (idIter = std::find_if(idIter, header.end(), [&](std::string const& c) {
         return c.rfind(kIdColumnPrefix) == 0;
       })) != header.end()) {
    idColumnIndices.push_back(std::distance(header.begin(), idIter));
    idIter++;
  }
  if (0 == idColumnIndices.size()) {
    // note: it's not *essential* to clean up tmpfile here, but it will
    // pollute our test directory otherwise, which is just somewhat annoying.
    std::remove(tmpFilename.c_str());
    XLOG(FATAL) << kIdColumnPrefix << " column missing from input header\n"
                << "Header: " << vectorToString(header);
  }

  std::unordered_set<std::string> seenIds;
  while (getline(inStream, line)) {
    auto cols = splitByCharDelimiter(line, ',');
    // auto cols = split(line, kCommaSplitRegex);
    auto rowSize = cols.size();
    auto headerSize = header.size();

    if (rowSize != headerSize) {
      // note: it's not *essential* to clean up tmpfile here, but it will
      // pollute our test directory otherwise, which is just somewhat annoying.
      std::remove(tmpFilename.c_str());
      XLOG(FATAL) << "Mismatch between header and row at index "
                  << res.linesProcessed << '\n'
                  << "Header has size " << headerSize << " while row has size "
                  << rowSize << '\n'
                  << "Header: " << vectorToString(header) << '\n'
                  << "Row   : " << vectorToString(cols);
    }

    // Stores non-null id values in vector ids.
    // Duplicate ids are not allowed. If we find duplicates, we skip this row.
    bool duplicateFlag = false;
    std::vector<std::string> ids;
    for (std::int64_t idColumnIdx : idColumnIndices) {
      auto id = cols.at(idColumnIdx);
      if (id == "") {
        continue;
      }
      if (seenIds.find(id) != seenIds.end()) {
        duplicateFlag = true;
        ++res.duplicateIdCount;
        break;
      }
      ids.push_back(id);
      if (ids.size() == maxColumnCnt_) {
        break;
      }
    }

    // skip if number of ids == 0 or duplicateFlag == true
    if (ids.size() > 0 && !duplicateFlag) {
      // only when row is not skipped we put ids into seenIds
      for (auto id : ids) {
        seenIds.insert(id);
      }
      // join all the ids with delimiter ","
      std::string concatIds = std::accumulate(
          ids.begin(),
          ids.end(),
          std::string(),
          [&](std::string x, std::string y) {
            return x.empty() ? y : x + std::string(",") + y;
          });

      *tmpFile << concatIds << '\n';
      seenIds.insert(concatIds);
    }

    ++res.linesProcessed;
    if (res.linesProcessed % logEveryN_ == 0) {
      XLOG(INFO) << "Processed "
                 << private_lift::logging::formatNumber(res.linesProcessed)
                 << " lines.";
    }
  }
  XLOG(INFO) << "Processed with "
             << private_lift::logging::formatNumber(res.duplicateIdCount)
             << " duplicate ids.";

  if (res.linesProcessed == 0) {
    XLOG(INFO) << "The file is empty. Adding random dummy row";
    // Using random value to avoid accidental match with other-side data
    auto randomDummyRow = std::to_string(folly::Random::secureRand64());
    *tmpFile << randomDummyRow << "\n";
  }

  XLOG(INFO) << "Now copying prepared data to final output path";
  // Reset underlying unique_ptr to ensure buffer gets flushed
  tmpFile.reset();
  XLOG(INFO) << "Writing " << tmpFilename << " -> " << outputPath_;

  auto outputType = fbpcf::io::getFileType(outputPath_);
  if (outputType == fbpcf::io::FileType::S3) {
    private_lift::s3_utils::uploadToS3(tmpFilename, outputPath_);
  } else if (outputType == fbpcf::io::FileType::Local) {
    std::filesystem::copy(
        tmpFilename,
        outputPath_,
        std::filesystem::copy_options::overwrite_existing);
  } else {
    throw std::runtime_error{"Unsupported output destination"};
  }
  // We need to make sure we clean up the tmpfiles now
  std::remove(tmpFilename.c_str());
  XLOG(INFO) << "File write successful.";

  return res;
}

} // namespace measurement::pid
