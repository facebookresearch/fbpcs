/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "SortIntegralValues.h"

#include <folly/String.h>
#include <folly/logging/xlog.h>

#include <istream>
#include <ostream>
#include <string>
#include <vector>

#include "DataPreparationHelpers.h"

// TODO(T90086783): We should rely upon Csv.h to handle this sort of parsing for
// us
namespace {
std::vector<std::string> splitWithBrackets(const std::string& s) {
  std::vector<std::string> res;
  std::size_t start = 0;
  bool inBrackets = false;
  for (std::size_t i = 0; i < s.size(); ++i) {
    if (s.at(i) == ',' && !inBrackets) {
      res.push_back(s.substr(start, i - start));
      start = i + 1;
    } else if (s.at(i) == '[') {
      inBrackets = true;
    } else if (s.at(i) == ']') {
      inBrackets = false;
    }
  }
  // Remember to include the last column
  res.push_back(s.substr(start));
  return res;
}
} // namespace

namespace pid::combiner {
void sortIntegralValues(
    std::istream& inStream,
    std::ostream& outStream,
    const std::string& sortBy,
    const std::vector<std::string>& listColumns) {
  if (std::find(listColumns.begin(), listColumns.end(), sortBy) ==
      listColumns.end()) {
    XLOG(FATAL) << "SortBy column must be contained in the listColumns";
  }

  std::string line;
  getline(inStream, line);
  auto header = splitWithBrackets(line);

  auto headerSize = header.size();

  // Output the header as before
  outStream << vectorToString(header) << '\n';

  while (getline(inStream, line)) {
    auto row = splitWithBrackets(line);
    auto rowSize = row.size();
    if (rowSize != headerSize) {
      XLOG(FATAL) << "Mismatch between header and row\n"
                  << "Header has size " << headerSize << " while row has size "
                  << rowSize << '\n'
                  << "Header: " << vectorToString(header) << '\n'
                  << "Row   : " << vectorToString(row) << '\n';
    }

    // First parse the listy columns
    std::vector<std::vector<std::string>> listsInRow;
    std::size_t sortByIdxInParsedLists;
    std::size_t pushBackIdx = 0;
    for (const auto& listCol : listColumns) {
      if (listCol == sortBy) {
        sortByIdxInParsedLists = pushBackIdx;
      }
      auto idx = headerIndex(header, listCol);
      listsInRow.push_back(splitList(row.at(idx)));
      ++pushBackIdx;
    }

    // We go ahead and parse the sortBy column once to avoid duplicating work
    std::vector<int64_t> vals;
    for (const auto& s : listsInRow.at(sortByIdxInParsedLists)) {
      int64_t parsed;
      std::istringstream parser{s};
      parser >> parsed;
      if (parser.fail()) {
        XLOG(FATAL) << "Failed to parse " << s << " as int64_t";
      }
      vals.push_back(parsed);
    }

    // Then sort them all based on the sortBy column
    auto permutation =
        getSortPermutation(vals, [](int64_t a, int64_t b) { return a < b; });
    XLOG(INFO) << "The permutation of " << vectorToString(vals) << " is... "
               << vectorToString(permutation);

    // Apply the permutation to every list column
    for (auto& lst : listsInRow) {
      applyPermutation(lst, permutation);
    }

    // Finally, emit a new line
    bool first = true;
    for (std::size_t i = 0; i < row.size(); ++i) {
      if (!first) {
        outStream << ',';
      }
      first = false;

      // If this is a list column, output from the sorted listsInRow instead
      auto listFind =
          std::find(listColumns.begin(), listColumns.end(), header.at(i));
      if (listFind != listColumns.end()) {
        outStream << '['
                  << vectorToString(
                         listsInRow.at(listFind - listColumns.begin()))
                  << ']';
      } else {
        // Otherwise we have the "easy" case -- just output
        outStream << row.at(i);
      }
    }
    outStream << '\n';
  }
}
} // namespace pid::combiner
