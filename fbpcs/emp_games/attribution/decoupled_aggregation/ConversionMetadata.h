/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <emp-sh2pc/emp-sh2pc.h>
#include <fbpcf/mpc/EmpGame.h>
#include <string>
#include <unordered_set>
#include <vector>
#include "folly/json.h"
#include "folly/logging/xlog.h"

#include "fbpcs/emp_games/attribution/decoupled_aggregation/Constants.h"

namespace aggregation::private_aggregation {

struct ConversionMetadata {
  int64_t ts;
  int32_t conv_value;
  int32_t conv_metadata;

  bool operator<(const ConversionMetadata& cvm) const {
    return (ts < cvm.ts);
  }
};

struct MeasurementConversionMetadata {
  const int32_t conv_value;

  // privatelyShareArrayFrom support
  friend bool operator==(
      const MeasurementConversionMetadata& a,
      const MeasurementConversionMetadata& b) {
    return a.conv_value == b.conv_value;
  }
  friend std::ostream& operator<<(
      std::ostream& os,
      const MeasurementConversionMetadata& conv) {
    return os << "Measurement Conversion {value=" << conv.conv_value << "}";
  }
};

struct PrivateMeasurementConversionMetadata {
  emp::Integer conv_value;

  explicit PrivateMeasurementConversionMetadata(const emp::Integer& _conv_value)
      : conv_value{_conv_value} {}

  // emp::batcher based construction support
  PrivateMeasurementConversionMetadata(int len, const emp::block* b)
      : conv_value{INT_SIZE_32, b} {}

  // string conversion support
  template <typename T = std::string>
  T reveal(int party = emp::PUBLIC) const {
    std::stringstream out;
    out << "Measurement Conversion {value=";
    out << conv_value.reveal<int32_t>(party);
    out << "}";

    return out.str();
  }

  // emp::batcher serialization support
  template <typename... Args>
  static size_t bool_size(Args...) {
    return emp::Integer::bool_size(INT_SIZE_32, 0 /* dummy value */);
  }

  // emp::batcher serialization support
  static void bool_data(bool* data, const MeasurementConversionMetadata& conv) {
    emp::Integer::bool_data(data, INT_SIZE_32, conv.conv_value);
  }
};

} // namespace aggregation::private_aggregation
