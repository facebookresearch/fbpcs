/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "fbpcs/data_processing/sharding/GenericSharder.h"

#include <algorithm>
#include <string>
#include <vector>

namespace data_processing::sharder {
namespace detail {
void stripQuotes(std::string &s) {
  s.erase(std::remove(s.begin(), s.end(), '"'), s.end());
}
} // namespace detail

std::vector<std::string>
GenericSharder::genOutputPaths(const std::string &outputBasePath,
                               std::size_t startIndex, std::size_t endIndex) {
  std::vector<std::string> res;
  for (std::size_t i = startIndex; i < endIndex; ++i) {
    res.push_back(outputBasePath + '_' + std::to_string(i));
  }
  return res;
}
} // namespace data_processing::sharder
