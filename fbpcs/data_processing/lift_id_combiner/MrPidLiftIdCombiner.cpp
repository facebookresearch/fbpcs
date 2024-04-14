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
#include <unordered_set>
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
  XLOG(INFO) << "Starting attribution id combiner run on: " << "spine_path: "
             << spineIdFilePath << ", output_path: " << outputStr
             << ", tmp_directory: " << tmpDirectory
             << ", sorting_strategy: " << sortStrategy
             << ", max_id_column_cnt: " << maxIdColumnCnt
             << ", protocol_type: " << protocolType;

  auto spineReader = std::make_unique<fbpcf::io::FileReader>(FLAGS_spine_path);
  spineIdFile =
      std::make_shared<fbpcf::io::BufferedReader>(std::move(spineReader));
}

MrPidLiftIdCombiner::~MrPidLiftIdCombiner() {
  spineIdFile->close();
}

std::stringstream MrPidLiftIdCombiner::idSwap(FileMetaData meta) {
  auto spineReader = std::make_unique<fbpcf::io::FileReader>(spineIdFilePath);
  auto spineIdFileDup =
      std::make_shared<fbpcf::io::BufferedReader>(std::move(spineReader));

  std::stringstream idSwapOutFile;

  if (meta.isPublisherDataset) {
    const std::string kCommaSplitRegex = ",";
    const std::string kIdColumnPrefix = "id_";
    std::vector<std::string> header;
    folly::split(kCommaSplitRegex, meta.headerLine, header);
    // Build a map for <pid to data> from spine file
    std::unordered_map<std::string, std::vector<std::vector<std::string>>>
        pidToDataMap;
    // find id_ index in the header
    auto idColumnIndices = headerIndices(header, kIdColumnPrefix);
    if (idColumnIndices.size() == 0) {
      XLOG(FATAL) << "Cannot find the id_ in the header.";
    }
    auto idx = idColumnIndices[0]; // NOLINT
    // remove the id column and add pid column in the beginning
    header.erase(header.begin() + idx);
    header.insert(header.begin(), "id_");

    idSwapOutFile << vectorToString(header) << "\n";
    while (!spineIdFile->eof()) {
      auto line = spineIdFile->readLine();
      std::vector<std::string> rowVec;
      folly::split(kCommaSplitRegex, line, rowVec);
      // for each row in spine id,
      // look for the corresponding rows in spineFile and
      // output the private_id, along with the data from spineFile
      auto privId = rowVec.at(idx);
      rowVec.erase(rowVec.begin() + idx);

      pidToDataMap[privId].push_back(rowVec);
    }
    std::string row;
    std::unordered_set<std::string> pidVisited;
    // skip the header
    spineIdFileDup->readLine();
    while (!spineIdFileDup->eof()) {
      row = spineIdFileDup->readLine();
      std::vector<std::string> cols;
      folly::split(kCommaSplitRegex, row, cols);
      // get private id from position idx
      auto privId = cols.at(idx);

      if (pidVisited.find(privId) == pidVisited.end() &&
          pidToDataMap.find(privId) != pidToDataMap.end()) {
        auto& dRows = pidToDataMap.at(privId);

        // For publisher lift dataset, duplicates would result in failure.
        // We are aggregating columns here.
        if (dRows.size() > 1) {
          aggregateLiftNonIdColumns(header, dRows);
        }
        pidVisited.insert(privId);
        if (dRows.size() > 0) {
          idSwapOutFile << privId << "," << vectorToString(dRows[0]) << '\n';
        }
      }
    }
    spineIdFileDup->close();
  } else {
    idSwapOutFile << meta.headerLine << "\n";
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
