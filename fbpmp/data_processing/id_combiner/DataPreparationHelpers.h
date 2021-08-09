/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <folly/String.h>

#include <algorithm>
#include <filesystem>
#include <numeric>
#include <sstream>
#include <unordered_map>
#include <variant>
#include <vector>

namespace pid::combiner {
/*
This file supports a set of helper functions for manipulating private
measurement datasets
*/
void headerColumnsToPlural(
    std::istream& dataFile,
    std::vector<std::string> columnsToConvert,
    std::ostream& outFile);

std::vector<std::string> split(const std::string& delim, std::string& str);

std::string vectorToStringWithReplacement(
    const std::vector<std::string>& vec,
    size_t swapIndex,
    std::string swapValue);

size_t headerIndex(
    const std::vector<std::string>& header,
    const std::string& columnName);

std::vector<std::string> splitList(const std::string& s);

// From https://stackoverflow.com/questions/17074324/
template <typename T, typename Compare>
std::vector<std::size_t> getSortPermutation(
    const std::vector<T>& vec,
    Compare compare) {
  std::vector<std::size_t> p(vec.size());
  std::iota(p.begin(), p.end(), 0);
  std::sort(p.begin(), p.end(), [&](std::size_t i, std::size_t j) {
    return compare(vec.at(i), vec.at(j));
  });
  return p;
}

// From https://stackoverflow.com/questions/17074324/
template <typename T>
void applyPermutation(std::vector<T>& vec, const std::vector<std::size_t>& p) {
  std::vector<bool> done(vec.size());
  for (std::size_t i = 0; i < vec.size(); ++i) {
    if (done.at(i)) {
      continue;
    }
    done.at(i) = true;
    std::size_t prev_j = i;
    std::size_t j = p.at(i);
    while (i != j) {
      std::swap(vec.at(prev_j), vec.at(j));
      done.at(j) = true;
      prev_j = j;
      j = p.at(j);
    }
  }
}

template <typename T>
const std::string vectorToString(const std::vector<T>& vec) {
  std::stringstream buf;
  bool first = true;
  for (int i = 0; i < vec.size(); ++i) {
    if (!first) {
      buf << ",";
    }
    buf << vec.at(i);
    first = false;
  }
  return buf.str();
}
} // namespace pid::combiner
