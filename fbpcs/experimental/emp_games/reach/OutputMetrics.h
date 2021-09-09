/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <ostream>
#include <unordered_map>

namespace measurement::private_reach {

struct OutputMetrics {
  int64_t reach;
  std::unordered_map<int64_t, int64_t> frequencyHistogram;

  friend std::ostream& operator<<(std::ostream& os, const OutputMetrics& m) {
    os << "Reach: " << m.reach << '\n';
    os << "Frequency histogram: {";
    bool first = true;
    for (const auto& [key, val] : m.frequencyHistogram) {
      if (!first) {
        os << ", ";
      }
      os << '[' << key << ": " << val << ']';
      first = false;
    }
    os << '}';
    return os;
  }
};

} // namespace measurement::private_reach
