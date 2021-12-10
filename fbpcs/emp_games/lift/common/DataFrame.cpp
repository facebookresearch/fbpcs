/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "fbpcs/emp_games/lift/common/DataFrame.h"

#include <algorithm>
#include <string>
#include <vector>

#include "fbpcs/emp_games/lift/common/CsvReader.h"

namespace df {
DataFrame DataFrame::readCsv(
    const TypeMap& typeMap,
    const std::string& filePath) {
  CsvReader rdr{filePath};
  return loadFromRows(typeMap, rdr.getHeader(), rdr.getRows());
}

DataFrame DataFrame::loadFromRows(
    const TypeMap& typeMap,
    const std::vector<std::string>& header,
    const std::vector<std::vector<std::string>>& rows) {
  DataFrame df;

  for (const auto& row : rows) {
    for (std::size_t i = 0; i < row.size(); ++i) {
      auto colName = header.at(i);
      if (std::find(
              typeMap.boolColumns.begin(),
              typeMap.boolColumns.end(),
              colName) != typeMap.boolColumns.end()) {
        df.get<bool>(colName).push_back(detail::parse<bool>(row.at(i)));
      } else if (
          std::find(
              typeMap.intColumns.begin(), typeMap.intColumns.end(), colName) !=
          typeMap.intColumns.end()) {
        df.get<int64_t>(colName).push_back(detail::parse<int64_t>(row.at(i)));
      } else if (
          std::find(
              typeMap.intVecColumns.begin(),
              typeMap.intVecColumns.end(),
              colName) != typeMap.intVecColumns.end()) {
        df.get<std::vector<int64_t>>(colName).push_back(
            detail::parseVector<int64_t>(row.at(i)));
      } else {
        // Either we don't know what this column is, or it's supposed to be a
        // string anyway. Safest approach is to not do *any* parsing in this
        // case.
        df.get<std::string>(colName).push_back(row.at(i));
      }
    }
  }

  return df;
}
} // namespace df
