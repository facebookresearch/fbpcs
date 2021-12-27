/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "IdInsert.h"
#include "DataPreparationHelpers.h"

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
void idInsert(
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

  // Output the header for the data
  outFile << vectorToString(header) << "\n";

  // Build a map for <private_id to data> from the mapped data file
  // to ensure every existing privateId is captured
  std::unordered_map<std::string, std::vector<std::vector<std::string>>>
      pidToDataMap;
  std::vector<std::string> dataRow;
  while (getline(dataFile, line)) {
    std::vector<std::string> rowVec;
    folly::split(kCommaSplitRegex, line, rowVec);
    pidToDataMap[rowVec.at(idColumnIdx)].push_back(rowVec);
  }

  // Output each row from mappedDataFile to outFile
  // if private_id doesnt exist put in the default row
  std::string row;
  while (getline(spineIdFile, row)) {
    std::vector<std::string> cols;
    folly::split(kCommaSplitRegex, row, cols);

    // for each row in spine id,
    // look for the corresponding rows in mappedDataFile and output the data
    // if the pid does not exist in the mappedDataFile, write the row with 0s
    auto priv_id = cols.at(0);

    if (pidToDataMap.count(priv_id) > 0) {
      auto& dataRows = pidToDataMap.at(priv_id);
      for (auto& dRow : dataRows) {
        outFile << vectorToString(dRow) << '\n';
      }
    } else {
      std::vector<std::string> defaultVector(headerSize, "0");
      outFile << vectorToStringWithReplacement(
                     defaultVector, idColumnIdx, priv_id)
              << '\n';
    }
  }

  XLOG(INFO) << "Finished.";
}
} // namespace pid::combiner
