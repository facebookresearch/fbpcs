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

#include <folly/String.h>
#include "fbpcf/io/api/BufferedReader.h"
#include "fbpcf/io/api/FileReader.h"
#include "folly/Random.h"
#include "folly/logging/xlog.h"

// TODO: Rewrite for OSS?
#include "../common/FilepathHelpers.h"
#include "../common/Logging.h"
#include "../common/S3CopyFromLocalUtil.h"
#include "fbpcf/io/FileManagerUtil.h"
#include "fbpcf/io/api/FileIOWrappers.h"

namespace measurement::pid {

static const std::string kIdColumnPrefix = "id_";

UnionPIDDataPreparerResults UnionPIDDataPreparer::prepare() const {
  UnionPIDDataPreparerResults res;
  auto reader = std::make_unique<fbpcf::io::FileReader>(inputPath_);
  auto bufferedReader = std::make_unique<fbpcf::io::BufferedReader>(
      std::move(reader), kBufferedReaderChunkSize);

  // Get a random ID to avoid potential name collisions if multiple
  // runs at the same time point to the same input file
  auto randomId = std::to_string(folly::Random::secureRand64());
  std::string tmpFilename = randomId + "_" +
      private_lift::filepath_helpers::getBaseFilename(inputPath_) + "_prepared";
  auto tmpFilepath = (tmpDirectory_ / tmpFilename).string();
  std::cout << "\t\tCreated temporary filepath --> " << tmpFilepath << '\n';
  auto tmpFile = std::make_unique<std::ofstream>(tmpFilename);

  std::vector<std::string> header;

  std::string line = bufferedReader->readLine();
  line.erase(std::remove(line.begin(), line.end(), ' '), line.end());
  folly::split(",", line, header);

  auto idIter = header.begin();
  std::vector<std::int64_t> idColumnIndices;

  // find indices of columns with its column name start with kIdColumnPrefix
  auto hasIdColumnPrefix = [&](std::string const& c) {
    return c.rfind(kIdColumnPrefix) == 0;
  };
  while ((idIter = std::find_if(idIter, header.end(), hasIdColumnPrefix)) !=
         header.end()) {
    idColumnIndices.push_back(std::distance(header.begin(), idIter));
    idIter++;
  }
  if (0 == idColumnIndices.size()) {
    // note: it's not *essential* to clean up tmpfile here, but it will
    // pollute our test directory otherwise, which is just somewhat annoying.
    std::remove(tmpFilename.c_str());
    XLOG(FATAL) << kIdColumnPrefix
                << " prefixed-column missing from input header"
                << "Header: [" << folly::join(",", header) << "]";
  }

  std::unordered_set<std::string> seenIds;
  while (!bufferedReader->eof()) {
    line = bufferedReader->readLine();
    std::vector<std::string> cols;
    line.erase(std::remove(line.begin(), line.end(), ' '), line.end());
    folly::split(",", line, cols);
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
                  << "Header: [" << folly::join(",", header) << "]\n"
                  << "Row   : [" << folly::join(",", header) << "]";
    }

    // Stores non-null id values in vector ids.
    // Duplicate ids are not allowed. If we find duplicates, we skip this row.
    bool isDuplicateRow = false;
    std::vector<std::string> ids;
    for (std::int64_t idColumnIdx : idColumnIndices) {
      auto id = cols.at(idColumnIdx);
      if (id == "") {
        continue;
      }
      if (seenIds.find(id) != seenIds.end()) {
        isDuplicateRow = true;
        ++res.duplicateIdCount;
        break;
      }
      ids.push_back(id);
      if (ids.size() == maxColumnCnt_) {
        break;
      }
    }

    // skip if number of ids == 0 or identifiers is already present in other row
    if (ids.size() > 0 && !isDuplicateRow) {
      // only when row is not skipped we put ids into seenIds
      for (auto id : ids) {
        seenIds.insert(id);
      }

      // join all the ids with delimiter ","
      *tmpFile << folly::join(",", ids) << '\n';
    }

    ++res.linesProcessed;
    if (res.linesProcessed % logEveryN_ == 0) {
      XLOG(INFO) << "Processed "
                 << private_lift::logging::formatNumber(res.linesProcessed)
                 << " lines.";
    }
  }
  bufferedReader->close();
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
  fbpcf::io::FileIOWrappers::transferFileInParts(tmpFilename, outputPath_);
  // We need to make sure we clean up the tmpfiles now
  std::remove(tmpFilename.c_str());
  XLOG(INFO) << "File write successful.";

  return res;
}

} // namespace measurement::pid
