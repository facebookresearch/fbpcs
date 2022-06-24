/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "IdSwapMultiKey.h"
#include "DataPreparationHelpers.h"
#include "folly/Optional.h"

#include <filesystem>
#include <fstream>
#include <iomanip>
#include <istream>
#include <numeric>
#include <ostream>
#include <sstream>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <vector>

#include <folly/String.h>
#include <folly/logging/xlog.h>
#include <re2/re2.h>

namespace pid::combiner {
void idSwapMultiKey(
    std::shared_ptr<fbpcf::io::BufferedReader> dataFile,
    std::shared_ptr<fbpcf::io::BufferedReader> spineIdFile,
    std::ostream& outFile,
    int32_t maxIdColumnCnt,
    std::string headerLine,
    std::string spineIdPath) {
  const std::string kCommaSplitRegex = ",";
  const std::string kIdColumnPrefix = "id_";
  const std::string kDefaultNullReplacement = "0";

  XLOG(INFO) << "Starting.";

  std::string line;
  /*
   * Need to create a duplicate Reader for Spine Input File path as its being
   * read multiple times in this method. However, this may end up being
   * inefficient because then we would make two requests to AWS instead of 1.
   */
  auto spineReader = std::make_unique<fbpcf::io::FileReader>(spineIdPath);
  auto spineIdFileDup = std::make_shared<fbpcf::io::BufferedReader>(
      std::move(spineReader), pid::combiner::kBufferedReaderChunkSize);
  std::vector<std::string> header;
  folly::split(kCommaSplitRegex, headerLine, header);

  auto idColumnIndices = headerIndices(header, kIdColumnPrefix);

  auto headerSize = header.size();

  // remove all the id columns and add pid column in the beginning
  for (int i = 0; i < idColumnIndices.size(); i++) {
    header.erase(header.begin() + idColumnIndices.at(i) - i);
  }
  header.insert(header.begin(), "id_");
  outFile << vectorToString(header) << "\n";

  // Build a map for <id_ to private_id> from the spineId File
  std::unordered_map<std::string, std::string> idToPrivateIDMap;
  std::string spineRow;
  while (!spineIdFile->eof()) {
    spineRow = spineIdFile->readLine();
    std::vector<std::string> cols;
    folly::split(kCommaSplitRegex, spineRow, cols);
    // expect col 1 in spineIdFile to contain the id_
    auto privId = cols.at(0);

    auto numCols = cols.size();
    if (numCols == 1) {
      continue;
    }

    // PID protocol does not yet allow the same identifiers
    // appearing in the multiple rows. Therefore, we store
    // all the identifiers in the row to idToPrivateIDMap as a key
    // and private id as its value.
    // It starts from i = 1 to skip private id column
    for (size_t i = 1; i < numCols; ++i) {
      auto& id = cols.at(i);
      if (id == "NA" || id == "") {
        // Some private id protocol would return 'NA' as identifier
        // if identifiers mapped to this private id does not exist
        continue;
      }
      idToPrivateIDMap[id] = privId;
    }
  }

  // Build a map for <pid to data> from data file
  std::unordered_map<std::string, std::vector<std::vector<std::string>>>
      pidToDataMap;

  while (!dataFile->eof()) {
    line = dataFile->readLine();
    std::vector<std::string> rowVec;
    folly::split(kCommaSplitRegex, line, rowVec);

    auto rowSize = rowVec.size();
    if (rowSize != headerSize) {
      XLOG(INFO) << "Mismatch between header and row '\n'"
                 << "Header has size " << headerSize << " while row has size "
                 << rowSize << '\n'
                 << "row: " << vectorToString(rowVec) << "\n"
                 << "header: " << vectorToString(header) << "\n";
      std::exit(1);
    }

    // copy vector of row and delete ids
    std::vector<std::string> dataRow = rowVec;
    for (size_t i = 0; i < idColumnIndices.size(); ++i) {
      // On iteration i, we already deleted i columns before
      // so we need to delete a {idColumnIndices.at(i) - i}th column
      dataRow.erase(dataRow.begin() + idColumnIndices.at(i) - i);
    }

    // check if an id has pid allocated.
    // if it does, store non-id row vector as value and pid allocated id as key
    int32_t numIds = 0;
    std::vector<std::string> rowIds;
    auto idSearch = idToPrivateIDMap.begin();
    for (auto idx : idColumnIndices) {
      auto id = rowVec.at(idx);
      if (id == "") {
        continue;
      }
      rowIds.push_back(id);
      idSearch = idToPrivateIDMap.find(id);
      if (idSearch != idToPrivateIDMap.end()) {
        auto pid = idToPrivateIDMap.at(id);
        pidToDataMap[pid].push_back(dataRow);
        break;
      }
      if (++numIds == maxIdColumnCnt) {
        break;
      }
    }

    // If there are no ids, just skip the row
    if (numIds == 0) {
      continue;
    }

    // make sure one of the keys in the row have
    // private_id mapped in the spineFile else throwing
    if (idSearch == idToPrivateIDMap.end()) {
      XLOG(FATAL) << "ID is missing in the spineID file '\n'"
                  << vectorToString(rowIds)
                  << " does not have a corresponding private_id"
                  << "\n";
    }
  }

  // Here we output each row from dataFile to outFile.
  std::string row;
  while (!spineIdFileDup->eof()) {
    row = spineIdFileDup->readLine();
    std::vector<std::string> cols;
    folly::split(kCommaSplitRegex, row, cols);

    // for each row in spine id,
    // look for the corresponding rows in dataFile and
    // output the private_id, along with the data from dataFile
    auto privId = cols.at(0);
    auto numIds = cols.size() - 1;
    auto numNonIds = headerSize - idColumnIndices.size();
    std::vector<std::string> defaultVector(numNonIds, kDefaultNullReplacement);
    auto defaultVectorString = vectorToString(defaultVector);

    if (numIds == 0) {
      // if corresponding row with private id does not exist,
      // we give 0s
      outFile << privId << "," << defaultVectorString << "\n";
      continue;
    }

    // add private id in the left most column (column name = id_)
    // add dataRow which has id columns stripped after private id column
    if (pidToDataMap.count(privId) > 0) {
      for (auto& dRow : pidToDataMap.at(privId)) {
        outFile << privId << "," << vectorToString(dRow) << '\n';
      }
    } else {
      // when associated data does not exist,
      // we give 0s. e.g. identifier is NA
      outFile << privId << "," << defaultVectorString << "\n";
      continue;
    }
  }
  spineIdFileDup->close();
  XLOG(INFO) << "Finished.";
}
} // namespace pid::combiner
