/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "AddPaddingToCols.h"

#include <iomanip>
#include <istream>
#include <ostream>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <vector>

#include <boost/algorithm/string.hpp>
#include <folly/String.h>
#include <folly/logging/xlog.h>
#include <re2/re2.h>

#include "DataPreparationHelpers.h"

namespace pid::combiner {
void addPaddingToCols(
    std::istream& dataFile,
    const std::vector<std::string>& cols,
    const std::vector<int32_t>& padSizePerCol,
    bool enforceMax,
    std::ostream& outFile) {
  const std::string kCommaSplitRegex = R"(([^,]+),?)";
  const std::string kCommaWithBracketSplitRegex = R"((\[[^\]]+\]|[^,]+),?)";

  XLOG(INFO) << "Starting AddPaddingToCols run for columns: "
             << vectorToString(cols)
             << " with paddings of: " << vectorToString(padSizePerCol);

  std::string headerline;
  std::string row;

  getline(dataFile, headerline);
  boost::algorithm::trim_if(headerline, boost::is_any_of("\r"));
  std::vector<std::string> header = split(kCommaSplitRegex, headerline);

  // Output the header as is
  outFile << vectorToString(header) << "\n";

  std::vector<int> colsIndexesToPad;
  for (std::vector<std::string>::size_type i = 0; i < cols.size(); i++) {
    colsIndexesToPad.push_back(headerIndex(header, cols.at(i)));
  }

  while (getline(dataFile, row)) {
    std::vector<std::string> curr_cols =
        split(kCommaWithBracketSplitRegex, row);

    // for each row, go through the columns that we want to pad
    // and add the missing padding at the beginning of the vector
    for (std::size_t i = 0; i < colsIndexesToPad.size(); i++) {
      std::size_t c_i = colsIndexesToPad.at(i);
      boost::erase_all(curr_cols.at(c_i), "[");
      boost::erase_all(curr_cols.at(c_i), "]");
      std::vector<std::string> curr_vec =
          split(kCommaSplitRegex, curr_cols.at(c_i));

      if (curr_vec.size() > static_cast<std::size_t>(padSizePerCol.at(i)) && enforceMax) {
        auto truncate_size = curr_vec.size() - padSizePerCol.at(i);
        curr_vec.erase(curr_vec.end() - truncate_size, curr_vec.end());
      }

      if (curr_vec.size() < static_cast<std::size_t>(padSizePerCol.at(i))) {
        std::vector<std::string> padding(
            padSizePerCol.at(i) - curr_vec.size(), "0");
        curr_vec.insert(curr_vec.begin(), padding.begin(), padding.end());
      }
      curr_cols.at(c_i) = "[" + vectorToString(curr_vec) + "]";
    }
    outFile << vectorToString(curr_cols) << "\n";
  }

  XLOG(INFO) << "Finished.";
}
} // namespace pid::combiner
