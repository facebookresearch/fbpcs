/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <stdexcept>
#include <string>

#include <emp-sh2pc/emp-sh2pc.h>
#include "folly/logging/xlog.h"

#include "fbpcs/emp_games/attribution/decoupled_attribution/Constants.h"

namespace aggregation::private_attribution {

struct Touchpoint {
  int64_t id;
  bool isClick;
  int64_t ts;

  // privatelyShareArrayFrom support
  friend bool operator==(const Touchpoint& a, const Touchpoint& b) {
    return a.id == b.id;
  }
  friend std::ostream& operator<<(std::ostream& os, const Touchpoint& tp) {
    return os << (tp.isClick ? "Click{" : "View{") << "id=" << tp.id
              << ", ts=" << tp.ts << "}";
  }

  /**
   * If both are clicks, or both are views, the earliest one comes first.
   * If one is a click but the other is a view, the view comes first.
   */
  bool operator<(const Touchpoint& tp) const {
    return (isClick == tp.isClick) ? (ts < tp.ts) : !isClick;
  }
};

struct PrivateTouchpoint {
  emp::Bit isClick;
  emp::Integer ts;
  emp::Integer id;

  explicit PrivateTouchpoint(Touchpoint tp, int party)
      : PrivateTouchpoint(
            emp::Bit{tp.isClick, party},
            emp::Integer{TS_SIZE, tp.ts, party},
            emp::Integer{INT_SIZE, tp.id, party}) {}

  explicit PrivateTouchpoint(
      const emp::Bit& _isClick,
      const emp::Integer& _ts,
      const emp::Integer& _id)
      : isClick{_isClick}, ts{_ts}, id{_id} {}

  explicit PrivateTouchpoint()
      : isClick{false, emp::ALICE},
        ts{TS_SIZE, -1, emp::ALICE},
        id{INT_SIZE, INVALID_TP_ID, emp::ALICE} {}

  PrivateTouchpoint select(const emp::Bit& useRhs, const PrivateTouchpoint& rhs)
      const {
    return PrivateTouchpoint{
        /* isClick */ isClick.select(useRhs, rhs.isClick),
        /* ts */ ts.select(useRhs, rhs.ts),
        /* id */ id.select(useRhs, rhs.id)};
  }

  // Checking if timestamp > 0
  emp::Bit isValid() const {
    const emp::Integer one{TS_SIZE, 1};
    return ts >= one;
  }

  // string conversion support
  template <typename T = std::string>
  T reveal(int party) const {
    std::stringstream out;

    out << (isClick.reveal<bool>(party) ? "Click{" : "View{");
    out << "id=";
    out << id.reveal<int64_t>(party);
    out << ", ts=";
    out << ts.reveal<int64_t>(party);
    out << "}";

    return out.str();
  }
};

} // namespace aggregation::private_attribution
