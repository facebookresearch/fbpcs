/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "fbpcs/data_processing/id_combiner/IdSwapMultiKey.h"
#include "fbpcs/data_processing/id_combiner/DataPreparationHelpers.h"
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
    std::istream& dataFile,
    std::istream& spineIdFile,
    std::ostream& outFile,
    std::int32_t max_id_column_cnt) {
  const std::string kCommaSplitRegex = ",";
  const std::string kIdColumnPrefix = "id_";

  XLOG(INFO) << "Starting.";

  std::string line;

  getline(dataFile, line);
  std::vector<std::string> header;
  folly::split(kCommaSplitRegex, line, header);

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
  while (getline(spineIdFile, spineRow)) {
    std::vector<std::string> cols;
    folly::split(kCommaSplitRegex, spineRow, cols);
    // expect col 1 in spineIdFile to contain the id_
    auto priv_id = cols.at(0);
    auto numIds = cols.size() - 1;
    if (numIds == 0) {
      continue;
    }
    std::vector<std::string> row_ids;
    for (int i = 1; i < numIds + 1; i++) {
      row_ids.push_back(cols.at(i));
    }
    auto concat_id = std::accumulate(
        row_ids.begin(),
        row_ids.end(),
        std::string(),
        [&](std::string x, std::string y) {
          return x.empty() ? y : x + std::string(",") + y;
        });
    if (concat_id != "NA" && concat_id != "") {
      idToPrivateIDMap[concat_id] = priv_id;
    }
  }
  spineIdFile.clear();
  spineIdFile.seekg(0);

  // Build a map for <id_ to data> from data file
  std::unordered_map<std::string, std::vector<std::vector<std::string>>>
      idToDataMap;

  while (getline(dataFile, line)) {
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

    std::vector<std::string> row_ids;

    // use non-null values only to form id
    for (auto idx : idColumnIndices) {
      auto id = rowVec.at(idx);
      if (id == "") {
        continue;
      }
      row_ids.push_back(id);
      if (row_ids.size() == max_id_column_cnt) {
        break;
      }
    }
    if (row_ids.size() == 0) {
      continue;
    }
    auto concat_id = std::accumulate(
        row_ids.begin(),
        row_ids.end(),
        std::string(),
        [&](std::string x, std::string y) {
          return x.empty() ? y : x + std::string(",") + y;
        });

    // Verifying that every id in the dataFile has a corresponding
    // private_id mapped in the spineFile else throwing
    auto idSearch = idToPrivateIDMap.find(concat_id);
    if (idSearch == idToPrivateIDMap.end()) {
      XLOG(FATAL) << "ID is missing in the spineID file '\n'" << concat_id
                  << " does not have a corresponding private_id"
                  << "\n";
    }

    for (int i = 0; i < idColumnIndices.size(); i++) {
      rowVec.erase(rowVec.begin() + idColumnIndices.at(i) - i);
    }
    idToDataMap[concat_id].push_back(rowVec);
  }

  // Output each row from dataFile to outFile
  // ,keeping left most first id column and discarding others id columns
  // ,swapping out id_ for private_id_
  // if id_ doesn't exist in mapping/spineId file, throw an error
  std::string row;
  while (getline(spineIdFile, row)) {
    std::vector<std::string> cols;
    folly::split(kCommaSplitRegex, row, cols);

    // for each row in spine id,
    // look for the corresponding rows in dataFile and
    // output the private_id, along with the data from dataFile
    auto priv_id = cols.at(0);
    auto numIds = cols.size() - 1;
    auto numNonIds = headerSize - idColumnIndices.size();
    std::vector<std::string> defaultVector(numNonIds, "0");
    auto defaultVectorString = vectorToString(defaultVector);

    if (numIds == 0) {
      outFile << priv_id << "," << defaultVectorString << "\n";
      continue;
    }
    std::vector<std::string> row_ids;
    for (int i = 1; i < numIds + 1; i++) {
      row_ids.push_back(cols.at(i));
    }
    auto concat_id = std::accumulate(
        row_ids.begin(),
        row_ids.end(),
        std::string(),
        [&](std::string x, std::string y) {
          return x.empty() ? y : x + std::string(",") + y;
        });

    if (idToDataMap.count(concat_id) > 0) {
      auto& dataRows = idToDataMap.at(concat_id);
      for (auto& dRow : dataRows) {
        outFile << priv_id << "," << vectorToString(dRow) << '\n';
      }
    } else {
      outFile << priv_id << "," << defaultVectorString << "\n";
      continue;
    }
  }

  XLOG(INFO) << "Finished.";
}
} // namespace pid::combiner
