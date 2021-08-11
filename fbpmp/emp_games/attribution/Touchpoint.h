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
#include "Timestamp.h"

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
  Timestamp ts;
  emp::Integer id;
  emp::Integer campaignMetadata;

#define EMP_BIT_SIZE (static_cast<int>(emp::Bit::bool_size()))

  explicit PrivateTouchpoint(
      const emp::Bit& _isValid,
      const emp::Bit& _isClick,
      const emp::Integer& _adId,
      const Timestamp& _ts,
      const emp::Integer& _id,
      const emp::Integer& _campaignMetadata)
      : isValid{_isValid},
        isClick{_isClick},
        adId{_adId},
        ts{_ts},
        id{_id},
        campaignMetadata{_campaignMetadata} {}

  explicit PrivateTouchpoint()
      : isValid{false, emp::PUBLIC},
        isClick{false, emp::PUBLIC},
        adId{INT_SIZE, -1, emp::PUBLIC},
        ts{-1},
        id{INT_SIZE, INVALID_TP_ID, emp::PUBLIC},
        campaignMetadata{INT_SIZE, -1, emp::PUBLIC} {}

  // emp::batcher based construction support
  explicit PrivateTouchpoint(int len, const emp::block* b)
      // constructor for emp::Bit function happens to be a const emp::block&,
      // rather than emp::block* like other emp primitives. Making an explicit
      // static+cast is required for the compiler to select the right
      // constructor (otherwise the empty constructor is used).
      : isValid{static_cast<const emp::block&>(*b)},
        isClick{static_cast<const emp::block&>(*(b + EMP_BIT_SIZE))},
        // TODO there has to be a better way to do this addition, rather than
        // being forced to do it all inline?
        adId{INT_SIZE, b + 2 * EMP_BIT_SIZE},
        ts{b + 2 * EMP_BIT_SIZE + INT_SIZE},
        id{INT_SIZE, b + 2 * EMP_BIT_SIZE + INT_SIZE + ts.length()},
        campaignMetadata{
            INT_SIZE,
            b + 2 * EMP_BIT_SIZE + ts.length() + 2 * INT_SIZE} {}

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
    out << id.reveal<std::string>(party);
    out << ", adId=";
    out << adId.reveal<std::string>(party);
    out << ", ts=";
    out << ts.reveal<std::string>(party);
    out << ", campaignMetadata=";
    out << campaignMetadata.reveal<std::string>(party);
    out << "}";

    return out.str();
  }

  // emp::batcher serialization support
  template <typename... Args>
  static size_t bool_size(Args...) {
    return 2 * emp::Bit::bool_size() + Timestamp::bool_size() +
        3 * emp::Integer::bool_size(INT_SIZE, 0 /* dummy value */);
  }

  // emp::batcher serialization support
  static void bool_data(bool* data, const Touchpoint& tp) {
    auto offset = 0;

    emp::Bit::bool_data(data, tp.isValid());
    offset += emp::Bit::bool_size();

    emp::Bit::bool_data(data + offset, tp.isClick);
    offset += emp::Bit::bool_size();

    emp::Integer::bool_data(data + offset, INT_SIZE, tp.adId);
    offset += emp::Integer::bool_size(INT_SIZE, 0 /* dummy value */);

    Timestamp::bool_data(data + offset, tp.ts);
    offset += Timestamp::bool_size();

    emp::Integer::bool_data(data + offset, INT_SIZE, tp.id);
    offset += emp::Integer::bool_size(INT_SIZE, 0 /* dummy value */);

    emp::Integer::bool_data(data + offset, INT_SIZE, tp.campaignMetadata);
    offset += emp::Integer::bool_size(INT_SIZE, 0 /* dummy value */);
  }
};

} // namespace measurement::private_attribution
