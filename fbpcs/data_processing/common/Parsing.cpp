/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "Parsing.h"

#include <iomanip>
#include <istream>
#include <string>

#include <folly/logging/xlog.h>

namespace private_lift::parsing {

uint64_t parseStringToInt(const std::string& value) {
  uint64_t parsed = 0;
  std::istringstream iss{value};
  iss >> parsed;
  if (iss.fail()) {
    XLOG(INFO) << value
               << " in input file is not a number. Please validate your input.";
    throw std::out_of_range{iss.str()};
  }
  return parsed;
}

} // namespace private_lift::parsing
