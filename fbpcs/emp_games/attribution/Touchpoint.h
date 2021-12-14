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

#include "Constants.h"

namespace measurement::private_attribution {

struct Touchpoint {
  const int64_t id;
  const bool isClick;
  const int64_t adId;
  const int64_t ts;
  const int64_t campaignMetadata;

  // privatelyShareArrayFrom support
  friend bool operator==(const Touchpoint& a, const Touchpoint& b) {
    return a.id == b.id;
  }
  friend std::ostream& operator<<(std::ostream& os, const Touchpoint& tp) {
    return os << (tp.isClick ? "Click{" : "View{") << "id=" << tp.id
              << ", adId=" << tp.adId << ", ts=" << tp.ts
              << ", campaignMetadata=" << tp.campaignMetadata << "}";
  }

  bool isValid() const {
    return ts > 0;
  }
};

struct PrivateTouchpoint {
  emp::Bit isValid;
  emp::Bit isClick;
  emp::Integer adId;
  emp::Integer ts;
  emp::Integer id;
  emp::Integer campaignMetadata;

  explicit PrivateTouchpoint(Touchpoint tp, int party)
      : PrivateTouchpoint(
            emp::Bit{tp.isValid(), party},
            emp::Bit{tp.isClick, party},
            emp::Integer{INT_SIZE, tp.adId, party},
            emp::Integer{TS_SIZE, tp.ts, party},
            emp::Integer{INT_SIZE, tp.id, party},
            emp::Integer{INT_SIZE, tp.campaignMetadata, party}) {}

  explicit PrivateTouchpoint(
      const emp::Bit& _isValid,
      const emp::Bit& _isClick,
      const emp::Integer& _adId,
      const emp::Integer& _ts,
      const emp::Integer& _id,
      const emp::Integer& _campaignMetadata)
      : isValid{_isValid},
        isClick{_isClick},
        adId{_adId},
        ts{_ts},
        id{_id},
        campaignMetadata{_campaignMetadata} {}

  explicit PrivateTouchpoint()
      : isValid{false, emp::ALICE},
        isClick{false, emp::ALICE},
        adId{INT_SIZE, -1, emp::ALICE},
        ts{TS_SIZE, -1, emp::ALICE},
        id{INT_SIZE, INVALID_TP_ID, emp::ALICE},
        campaignMetadata{INT_SIZE, -1, emp::ALICE} {}

  PrivateTouchpoint select(const emp::Bit& useRhs, const PrivateTouchpoint& rhs)
      const {
    return PrivateTouchpoint{
        /* isValid */ isValid.select(useRhs, rhs.isValid),
        /* isClick */ isClick.select(useRhs, rhs.isClick),
        /* adId */ adId.select(useRhs, rhs.adId),
        /* ts */ ts.select(useRhs, rhs.ts),
        /* id */ id.select(useRhs, rhs.id),
        /* campaignMetadata */
        campaignMetadata.select(useRhs, rhs.campaignMetadata)};
  }

  // string conversion support
  template <typename T = std::string>
  T reveal(int party) const {
    std::stringstream out;

    out << (isClick.reveal<bool>(party) ? "Click{" : "View{");
    out << "id=";
    out << id.reveal<int64_t>(party);
    out << ", adId=";
    out << adId.reveal<int64_t>(party);
    out << ", ts=";
    out << ts.reveal<int64_t>(party);
    out << ", campaignMetadata=";
    out << campaignMetadata.reveal<int64_t>(party);
    out << "}";

    return out.str();
  }
};

} // namespace measurement::private_attribution
