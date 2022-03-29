/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "DataPreparationHelpers.h"

#include <folly/logging/xlog.h>
#include <cstdint>
#include <filesystem>
#include <iomanip>
#include <istream>
#include <ostream>
#include <sstream>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <vector>

#include <re2/re2.h>

namespace pid::combiner {

void headerColumnsToPlural(
    std::istream& dataFile,
    std::vector<std::string> columnsToConvert,
    std::ostream& outFile) {
  XLOG(INFO) << "Started converting columns to plural. Columns to convert: <"
             << vectorToString(columnsToConvert) << ">";

  const std::string kCommaSplitRegex = R"(([^,]+),?)";

  std::string line;
  std::string row;

  getline(dataFile, line);
  std::vector<std::string> header = split(kCommaSplitRegex, line);
  std::vector<std::string> newHeader;
  for (std::size_t i = 0; i < header.size(); i++) {
    auto useOriginalColumn = true;
    for (std::size_t j = 0; j < columnsToConvert.size(); j++) {
      if (header.at(i) == columnsToConvert.at(j)) {
        useOriginalColumn = false;
        newHeader.push_back(header.at(i) + "s");
      }
    }
    if (useOriginalColumn) {
      newHeader.push_back((header.at(i)));
    }
  }

  XLOG(INFO) << "New header: <" << vectorToString(newHeader) << ">";
  outFile << vectorToString(newHeader) << "\n";

  while (getline(dataFile, row)) {
    outFile << row << "\n";
  }
  XLOG(INFO) << "Finished converting header";
}
std::vector<std::string> split(const std::string& delim, std::string& str) {
  // Preprocessing step: Remove spaces if any
  str.erase(std::remove(str.begin(), str.end(), ' '), str.end());
  std::vector<std::string> tokens;
  re2::RE2 rgx{delim};
  re2::StringPiece input{str}; // Wrap a StringPiece around it

  std::string token;
  while (RE2::Consume(&input, rgx, &token)) {
    tokens.push_back(token);
  }
  return tokens;
}

std::vector<std::string> splitByComma(
    std::string& str,
    bool supportInnerBrackets) {
  if (supportInnerBrackets) {
    // The pattern here indicates that it's looking for a \[, gets all
    // non-brackets [^\]], then the \]. Otherwise |,
    // it will get all the non commas [^,]. The surrounding () makes it
    // a capture group. ,? means there may or may not be a comma
    return split(R"((\[[^\]]+\]|[^,]+),?)", str);
  } else {
    // split internally uses RE2 which relies on
    // consuming patterns. The pattern here indicates
    // it will get all the non commas [^,]. The surrounding () makes it
    // a capture group. ,? means there may or may not be a comma

    return split("([^,]+),?", str);
  }
}

size_t headerIndex(
    const std::vector<std::string>& header,
    const std::string& columnName) {
  auto idIter = std::find(header.begin(), header.end(), columnName);
  if (idIter == header.end()) {
    std::stringstream ss;
    ss << columnName << " column missing from input header\n";
    throw std::out_of_range{ss.str()};
  }
  return std::distance(header.begin(), idIter);
}

std::vector<std::int32_t> headerIndices(
    const std::vector<std::string>& header,
    const std::string& columnPrefix) {
  std::vector<std::int32_t> indices;
  auto idIter = header.begin();

  // find indices of columns with its column name start with columnPrefix
  while (
      (idIter = std::find_if(idIter, header.end(), [&](std::string const& c) {
         return c.rfind(columnPrefix) == 0;
       })) != header.end()) {
    indices.push_back(std::distance(header.begin(), idIter));
    idIter++;
  }
  return indices;
}

std::string vectorToStringWithReplacement(
    const std::vector<std::string>& vec,
    size_t swapIndex,
    std::string swapValue) {
  std::stringstream buf;
  bool first = true;
  for (std::size_t i = 0; i < vec.size(); ++i) {
    if (i == swapIndex) {
      if (!first) {
        buf << "," << swapValue;
      } else {
        buf << swapValue << ",";
      }
      continue;
    }

    if (!first) {
      buf << ",";
    }
    buf << vec.at(i);
    first = false;
  }
  return buf.str();
}

std::vector<std::string> splitList(const std::string& s) {
  const std::string kCommaSplitRegex = ",";
  // TODO: Check that first and last are [] characters
  // TODO: Using something like a stringview could make this more efficient
  // NOTE: we use -2 here because we want to exclude both the first and end char
  // and C++ substr uses "count" as the second parameter.
  auto innerString = s.substr(1, s.size() - 2);
  std::vector<std::string> res;
  folly::split(kCommaSplitRegex, innerString, res);
  return res;
}
} // namespace pid::combiner
