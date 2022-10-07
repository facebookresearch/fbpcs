/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <fbpcf/io/api/FileIOWrappers.h>
#include <filesystem>
#include <string>

#include <gtest/gtest.h>
#include "folly/Format.h"
#include "folly/Random.h"
#include "folly/logging/xlog.h"

namespace pcf2_dotproduct {

// result is expected to be in a one line list format (e.g., [0.3, 0.75, 0.1])
inline std::vector<double> parseResult(std::string filePath) {
  std::ifstream result;
  std::string line;

  result.open(filePath);
  std::getline(result, line);

  const auto left = line.find('[');
  const auto right = line.find(']');

  std::string valsString = line.substr(left + 1, right - (left + 1));

  std::vector<double> v;

  std::stringstream ss(valsString);

  while (ss.good()) {
    string substr;
    getline(ss, substr, ',');
    v.push_back(std::stod(substr));
  }
  return v;
}

// verify the dotproduct output with expected results, value difference should
// be smaller than 1e-7.
inline bool verifyOutput(
    std::vector<double> result,
    std::vector<double> expectedResult) {
  return std::equal(
      result.begin(),
      result.end(),
      expectedResult.begin(),
      [](double value1, double value2) {
        return std::fabs(value1 - value2) < 1e-7;
      });
}

} // namespace pcf2_dotproduct
