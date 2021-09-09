/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "IdSwap.h"
#include "DataPreparationHelpers.h"
#include "folly/Optional.h"

#include <filesystem>
#include <fstream>
#include <iomanip>
#include <istream>
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
void idSwap(
    std::istream& dataFile,
    std::istream& spineIdFile,
    std::ostream& outFile) {
  const std::string kCommaSplitRegex = ",";
  const std::string kIdColumnName = "id_";

  XLOG(INFO) << "Starting.";

  std::string line;

  getline(dataFile, line);
  std::vector<std::string> header;
  folly::split(kCommaSplitRegex, line, header);

  auto idColumnIdx = headerIndex(header, kIdColumnName);

  auto headerSize = header.size();

  // Output the header swapping out id_ for private_id_
  outFile << vectorToString(header) << "\n";

  // Build a map for <id_ to private_id> from the spineId File
  std::unordered_map<std::string, std::string> idToPrivateIDMap;
  std::string spineRow;
  while (getline(spineIdFile, spineRow)) {
    std::vector<std::string> cols;
    folly::split(kCommaSplitRegex, spineRow, cols);
    // expect col 1 in spineIdFile to contain the id_
    auto priv_id = cols.at(0);
    auto row_id = cols.at(1);
    if (row_id != "") {
      idToPrivateIDMap[row_id] = priv_id;
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
    // Verifying that every id in the dataFile has a corresponding
    // private_id mapped in the spineFile else throwing
    auto rowId = rowVec.at(idColumnIdx);
    auto idSearch = idToPrivateIDMap.find(rowId);
    if (idSearch == idToPrivateIDMap.end()) {
      XLOG(FATAL) << "ID is missing in the spineID file '\n'" << rowId
                  << " does not have a corresponding private_id"
                  << "\n";
    }

    idToDataMap[rowId].push_back(rowVec);
  }

  // Output each row from dataFile to outFile, swapping out id_ for private_id_
  // if id_ doesn't exist in mapping/spineId file, throw an error
  std::string row;
  while (getline(spineIdFile, row)) {
    std::vector<std::string> cols;
    folly::split(kCommaSplitRegex, row, cols);

    // for each row in spine id,
    // look for the corresponding rows in dataFile and
    // output the private_id, along with the data from dataFile
    auto priv_id = cols.at(0);
    auto row_id = cols.at(1); // expect col 1 in spineIdFile to contain the id_
    if (idToDataMap.count(row_id) > 0) {
      auto& dataRows = idToDataMap.at(row_id);
      for (auto& dRow : dataRows) {
        outFile << vectorToStringWithReplacement(dRow, idColumnIdx, priv_id)
                << '\n';
      }
    }
  }

  XLOG(INFO) << "Finished.";
}
} // namespace pid::combiner
