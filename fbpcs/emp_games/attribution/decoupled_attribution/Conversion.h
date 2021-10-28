/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <string>

#include <emp-sh2pc/emp-sh2pc.h>

#include "fbpcs/emp_games/attribution/decoupled_attribution/Constants.h"
#include "fbpcs/emp_games/attribution/decoupled_attribution/Timestamp.h"

namespace aggregation::private_attribution {

struct Conversion {
  int64_t ts;

  // privatelyShareArrayFrom support
  friend bool operator==(const Conversion& a, const Conversion& b) {
    return a.ts == b.ts;
  }
  friend std::ostream& operator<<(std::ostream& os, const Conversion& conv) {
    return os << "Conv{ts=" << conv.ts << "}";
  }
  bool operator<(const Conversion& conv) const {
    return (ts < conv.ts);
  }
};

struct PrivateConversion {
  Timestamp ts;

  explicit PrivateConversion( const Timestamp& _ts) : ts{_ts} {}

  // emp::batcher based construction support
  explicit PrivateConversion(int len, const emp::block* b) : ts{b} {}

  // string conversion support
  template <typename T = std::string>
  T reveal(int party = emp::PUBLIC) const {
    std::stringstream out;
    out << "Conv{ts=";
    out << ts.reveal<int64_t>(party);
    out << "}";

    return out.str();
  }

  // emp::batcher serialization support
  template <typename... Args>
  static size_t bool_size(Args...) {
    return Timestamp::bool_size();
  }

  // emp::batcher serialization support
  static void bool_data(bool* data, const Conversion& conv) {
    auto offset = 0;
    Timestamp::bool_data(data, conv.ts);
    offset += Timestamp::bool_size();
  }
};

} // namespace aggregation::private_attribution
