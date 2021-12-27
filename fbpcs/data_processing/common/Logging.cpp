/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <iomanip>
#include <sstream>
#include <string>

#include "Logging.h"

namespace private_lift::logging {

static constexpr int64_t kBillion = 1'000'000'000;
static constexpr int64_t kMillion = 1'000'000;
static constexpr int64_t kThousand = 1'000;

std::string formatNumber(uint64_t n) {
  if (n < kThousand) {
    return std::to_string(n);
  }

  const int64_t precision = 2;
  double base = kThousand;
  char unit = 'K';

  if (n >= kBillion) {
    base = kBillion;
    unit = 'B';
  } else if (n >= kMillion) {
    base = kMillion;
    unit = 'M';
  }

  const double value = double(n) / base;
  std::stringstream stream;
  stream << std::fixed << std::setprecision(precision) << value << unit;

  return stream.str();
}

} // namespace private_lift::logging
