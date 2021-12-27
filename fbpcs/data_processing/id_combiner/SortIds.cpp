/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "SortIds.h"

#include <filesystem>
#include <iomanip>
#include <istream>
#include <ostream>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <vector>

#include <folly/String.h>
#include <folly/logging/xlog.h>
#include <re2/re2.h>
#include "DataPreparationHelpers.h"

namespace pid::combiner {
void sortIds(std::istream& inFile, std::ostream& outFile) {
  const std::string kCommaSplitRegex = ",";
  const std::string kIdColumnName = "id_";

  std::string line;
  std::string row;

  getline(inFile, line);
  std::vector<std::string> header;
  folly::split(kCommaSplitRegex, line, header);

  auto headerSize = header.size();
  auto idColumnIdx = headerIndex(header, kIdColumnName);

  // Output the header as before
  outFile << vectorToString(header) << "\n";

  // Build a map for data value to row_id
  std::unordered_map<std::string, std::vector<std::string>> idToData;

  // Store the data map as well list of row_ids that need to be sorted
  std::vector<std::string> idList;
  while (getline(inFile, row)) {
    std::vector<std::string> cols;
    cols = splitByComma(row, true);
    auto rowSize = cols.size();
    if (rowSize != headerSize) {
      XLOG(FATAL) << "Mismatch between header and row" << '\n'
                  << "Header has size " << headerSize << " while row has size "
                  << rowSize << '\n'
                  << "Header: " << line << '\n'
                  << "Row   : " << row << '\n';
    }
    auto row_id = cols.at(idColumnIdx);
    idToData[row_id] = cols;
    idList.push_back(row_id);
  }

  sort(idList.begin(), idList.end());

  for (const auto& id : idList) {
    auto& dataRow = idToData.at(id);
    outFile << vectorToString(dataRow) << '\n';
  }

  XLOG(INFO) << "[C++ SortIds] Finished.\n";
}
} // namespace pid::combiner
