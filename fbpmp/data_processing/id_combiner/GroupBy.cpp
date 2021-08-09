/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "GroupBy.h"

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
void groupBy(
    std::istream& inFile,
    std::string groupByColumn,
    std::vector<std::string> columnsToAggregate,
    std::ostream& outFile) {
  const std::string kCommaSplitRegex = ",";

  XLOG(INFO) << "[C++ GroupBy] Starting GroupBy run to aggregate columns: "
             << vectorToString(columnsToAggregate)
             << " by column: " << groupByColumn << " \n";

  std::string line;
  std::string row;

  getline(inFile, line);
  std::vector<std::string> header;
  folly::split(kCommaSplitRegex, line, header);

  auto groupByColumnIndex = headerIndex(header, groupByColumn);

  auto headerSize = header.size();

  // Output the header as before
  outFile << vectorToString(header) << "\n";

  // Build a map for group_by_column value to row
  std::unordered_map<std::string, std::vector<std::vector<std::string>>>
      idToRows;

  while (getline(inFile, row)) {
    std::vector<std::string> cols;
    folly::split(kCommaSplitRegex, row, cols);
    auto rowSize = cols.size();
    if (rowSize != headerSize) {
      XLOG(FATAL) << "Mismatch between header and row" << '\n'
                  << "Header has size " << headerSize << " while row has size "
                  << rowSize << '\n'
                  << "Header: " << line << '\n'
                  << "Row   : " << row << '\n';
    }

    auto row_id = cols.at(groupByColumnIndex);
    idToRows[row_id].push_back(cols);
  }

  // build new map that aggregates common values into lists
  // newIdToRows maps the groupById to vectors of vectors
  // where the inner vector is an aggregation over col values
  // and the other vector represents all the columns for a given row
  std::map<std::string, std::vector<std::vector<std::string>>> newIdToRows;
  for (const auto& keyVal : idToRows) { // key value pairs of id to list of rows
    std::vector<std::vector<std::string>> newRow;
    std::vector<std::vector<std::string>> rows = keyVal.second;
    for (auto i = 0; i < rows.size(); i++) {
      auto curr_row = rows.at(i);
      for (auto j = 0; j < curr_row.size(); j++) {
        if (newRow.size() > j) {
          newRow.at(j).push_back(curr_row.at(j));
        } else {
          newRow.push_back({curr_row.at(j)});
        }
      }
    }
    newIdToRows[keyVal.first] = newRow;
  }

  // This loop iterates over all the unique rows and writes the aggregated
  // values to the output file
  // note that for columns that were not specified in columnsToAggregate, we
  // output a single value rather than a list of values
  for (const auto& keyVal : newIdToRows) {
    auto curr_row = keyVal.second;
    for (auto i = 0; i < curr_row.size(); i++) {
      if (std::find(
              columnsToAggregate.begin(),
              columnsToAggregate.end(),
              header.at(i)) != columnsToAggregate.end()) {
        outFile << "[" << vectorToString(curr_row.at(i)) << "]";
      } else { // just write out the first value in the list
        outFile << curr_row.at(i).at(0);
      }

      if (i < curr_row.size() - 1) {
        outFile << ",";
      }
    }
    outFile << "\n";
  }

  XLOG(INFO) << "[C++ GroupBy] Finished.\n";
}
} // namespace pid::combiner
