/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <string>

#include <emp-sh2pc/emp-sh2pc.h>

#include "fbpcs/emp_games/attribution/decoupled_attribution/Constants.h"

namespace aggregation::private_attribution {

struct Conversion {
  int64_t ts;
  int64_t targetId = -1;

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
  emp::Integer ts;

  explicit PrivateConversion(Conversion conv, int party)
      : ts{TS_SIZE, conv.ts, party} {}

  explicit PrivateConversion(const emp::Integer& _ts) : ts{_ts} {}

  // string conversion support
  template <typename T = std::string>
  T reveal(int party = emp::PUBLIC) const {
    std::stringstream out;
    out << "Conv{ts=";
    out << ts.reveal<int64_t>(party);
    out << "}";

    return out.str();
  }
};

} // namespace aggregation::private_attribution
