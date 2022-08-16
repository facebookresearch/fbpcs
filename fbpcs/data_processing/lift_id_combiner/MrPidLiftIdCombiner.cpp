/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "fbpcs/data_processing/lift_id_combiner/MrPidLiftIdCombiner.h"

#include <boost/algorithm/string.hpp>
#include <folly/Random.h>
#include <folly/String.h>
#include <folly/logging/xlog.h>
#include <re2/re2.h>
#include <iomanip>
#include <istream>
#include <ostream>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <vector>

#include "fbpcf/io/api/BufferedReader.h"
#include "fbpcf/io/api/FileIOWrappers.h"
#include "fbpcf/io/api/FileReader.h"
#include "fbpcs/data_processing/common/FilepathHelpers.h"
#include "fbpcs/data_processing/id_combiner/DataPreparationHelpers.h"
#include "fbpcs/data_processing/id_combiner/IdSwapMultiKey.h"
#include "fbpcs/data_processing/lift_id_combiner/LiftIdSpineCombinerOptions.h"

namespace pid::combiner {
MrPidLiftIdCombiner::MrPidLiftIdCombiner(
    std::string spineIdFilePath,
    std::string outputStr,
    std::string tmpDirectory,
    std::string sortStrategy,
    int maxIdColumnCnt,
    std::string protocolType)
    : spineIdFilePath(spineIdFilePath),
      tmpDirectory(tmpDirectory),
      sortStrategy(sortStrategy),
      maxIdColumnCnt(maxIdColumnCnt),
      outputPath{outputStr} {
  XLOG(INFO) << "Starting attribution id combiner run on: "
             << "spine_path: " << spineIdFilePath
             << ", output_path: " << outputStr
             << ", tmp_directory: " << tmpDirectory
             << ", sorting_strategy: " << sortStrategy
             << ", max_id_column_cnt: " << maxIdColumnCnt
             << ", protocol_type: " << protocolType;

  auto spineReader = std::make_unique<fbpcf::io::FileReader>(FLAGS_spine_path);
  spineIdFile = std::make_shared<fbpcf::io::BufferedReader>(
      std::move(spineReader), fbpcf::io::kBufferedReaderChunkSize);
}

MrPidLiftIdCombiner::~MrPidLiftIdCombiner() {
  spineIdFile->close();
}

std::stringstream MrPidLiftIdCombiner::idSwap(FileMetaData meta) {
  std::stringstream idSwapOutFile;
  idSwapOutFile << meta.headerLine << "\n";
  if (meta.isPublisherDataset) {
    const std::string kCommaSplitRegex = ",";
    std::vector<std::string> header;
    folly::split(kCommaSplitRegex, meta.headerLine, header);
    // Build a map for <pid to data> from data file
    std::unordered_map<std::string, std::vector<std::vector<std::string>>>
        pidToDataMap;

    while (!spineIdFile->eof()) {
      auto line = spineIdFile->readLine();
      std::vector<std::string> rowVec;
      folly::split(kCommaSplitRegex, line, rowVec);

      // expect col 1 in spineIdFile to contain the id_
      auto privId = rowVec.at(0);
      rowVec.erase(rowVec.begin());
      pidToDataMap[privId].push_back(rowVec);
    }
    for (auto& [privId, dRows] : pidToDataMap) {
      // For publisher lift dataset, duplicates would result in failure.
      // We are aggregating columns here.
      if (dRows.size() > 1) {
        aggregateLiftNonIdColumns(header, dRows);
      }

      idSwapOutFile << privId << "," << vectorToString(dRows[0]) << '\n';
    }

  } else {
    while (!spineIdFile->eof()) {
      auto spineRow = spineIdFile->readLine();
      idSwapOutFile << spineRow << "\n";
    }
  }

  return idSwapOutFile;
}

void MrPidLiftIdCombiner::run() {
  auto meta = processHeader(spineIdFile);
  std::stringstream idSwapOutFile = idSwap(meta);
  aggregate(
      idSwapOutFile,
      meta.isPublisherDataset,
      outputPath,
      tmpDirectory,
      sortStrategy);
}

} // namespace pid::combiner
