/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "fbpcs/emp_games/lift/common/CsvReader.h"

#include <filesystem>
#include <string>
#include <vector>

#include "fbpcf/io/FileManagerUtil.h"

#include "fbpcs/emp_games/lift/common/DataFrame.h"

namespace df {
namespace detail {
std::vector<std::string> split(const std::string& s) {
  std::vector<std::string> res;
  std::size_t i = 0;
  while (i < s.size()) {
    std::size_t lastStart = i;

    // Look for the end of this token
    if (s.at(i) == '[') {
      // Intentionally no size check
      // if we don't find ']', the string is malformed
      // NOTE: Nested brackets are undefined behavior
      while (s.at(i) != ']') {
        ++i;
      }
      // Since we *do* want to include the ']' character in our parsing,
      // we advance i one more now
      ++i;
    } else {
      while (i < s.size() && s.at(i) != ',') {
        ++i;
      }
    }
    res.push_back(s.substr(lastStart, i - lastStart));
    // Increment i so we're at the next valid character
    ++i;
  }
  return res;
}
} // namespace detail

CsvReader::CsvReader(const std::string& filePath) {
  auto infilePtr = fbpcf::io::getInputStream(filePath);
  auto& infile = infilePtr->get();
  if (!infile.good()) {
    throw CsvFileReadException{filePath};
  }

  std::string line;
  std::getline(infile, line);
  header_ = detail::split(line);

  while (std::getline(infile, line)) {
    auto nextRow = detail::split(line);
    if (header_.size() != nextRow.size()) {
      throw RowLengthMismatch{header_.size(), nextRow.size()};
    }
    rows_.push_back(std::move(nextRow));
  }
}
} // namespace df
