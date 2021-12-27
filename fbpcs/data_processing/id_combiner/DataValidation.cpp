/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "DataValidation.h"

#include <iomanip>
#include <istream>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <vector>

#include <boost/algorithm/string.hpp>
#include <folly/String.h>
#include <folly/logging/xlog.h>
#include <re2/re2.h>

#include "../common/Parsing.h"
#include "DataPreparationHelpers.h"

namespace pid::combiner {

bool verifyHeaderContainsCols(
    std::vector<std::string> header,
    std::vector<std::string> cols) {
  for (auto& colName : cols) {
    auto iter = std::find(header.begin(), header.end(), colName);
    if (iter == header.end()) {
      return false;
    }
  }
  return true;
}

void validateCsvData(std::istream& dataFile) {
  const std::string kCommaSplitRegex = R"(([^,]+),?)";

  XLOG(INFO) << "Started.";
  std::string line;
  std::string row;
  size_t row_i = 0;

  getline(dataFile, line);
  std::vector<std::string> header = split(kCommaSplitRegex, line);
  size_t headerSize = header.size();

  while (getline(dataFile, row)) {
    row_i++;
    std::vector<std::string> rowVec = split(kCommaSplitRegex, row);
    if (headerSize != rowVec.size()) {
      XLOG(FATAL) << "Row at index <" << row_i
                  << "> and header sizes mismatch. "
                  << "Row size is " << rowVec.size() << " and header size is "
                  << headerSize << ". Header: " << vectorToString(header);
    }
    for (auto& v : rowVec) {
      try {
        private_lift::parsing::parseStringToInt(v);
      } catch (std::exception& e) {
        XLOG(FATAL) << v << " failed to parse to int";
      }
    }
  }

  XLOG(INFO) << "Finished.";
}
} // namespace pid::combiner
