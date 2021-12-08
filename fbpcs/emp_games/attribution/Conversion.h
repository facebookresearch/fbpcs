/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <string>

#include <emp-sh2pc/emp-sh2pc.h>

#include "Constants.h"
#include "Timestamp.h"

namespace measurement::private_attribution {

struct Conversion {
  const int64_t ts;
  const int64_t conv_value;
  const int64_t conv_metadata;

  // privatelyShareArrayFrom support
  friend bool operator==(const Conversion& a, const Conversion& b) {
    return a.ts == b.ts && a.conv_value == b.conv_value;
  }
  friend std::ostream& operator<<(std::ostream& os, const Conversion& conv) {
    return os << "Conv{ts=" << conv.ts << ", value=" << conv.conv_value
              << ", metadata=" << conv.conv_metadata << "}";
  }
};

struct PrivateConversion {
  Timestamp ts;
  emp::Integer conv_value;
  emp::Integer conv_metadata;

  explicit PrivateConversion(Conversion conv, int party)
      : ts{conv.ts, party},
        conv_value{INT_SIZE, conv.conv_value, party},
        conv_metadata{INT_SIZE, conv.conv_metadata, party} {}

  PrivateConversion(
      const Timestamp& _ts,
      const emp::Integer& _conv_value,
      const emp::Integer& _conv_metadata)
      : ts{_ts}, conv_value{_conv_value}, conv_metadata{_conv_metadata} {}

  // string conversion support
  template <typename T = std::string>
  T reveal(int party = emp::PUBLIC) const {
    std::stringstream out;
    out << "Conv{ts=";
    out << ts.reveal<int64_t>(party);
    out << ", value=";
    out << conv_value.reveal<int64_t>(party);
    out << ", metadata=";
    out << conv_metadata.reveal<int64_t>(party);
    out << "}";

    return out.str();
  }
};

} // namespace measurement::private_attribution
