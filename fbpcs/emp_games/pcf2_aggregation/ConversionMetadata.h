/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include "fbpcs/emp_games/common/Constants.h"
#include "fbpcs/emp_games/pcf2_aggregation/Constants.h"

namespace pcf2_aggregation {

struct ConversionMetadata {
  uint64_t ts;
  uint32_t convValue;
  uint64_t convMetadata;
  common::InputEncryption inputEncryption;

  bool operator<(const ConversionMetadata& cvm) const {
    return (ts < cvm.ts);
  }
};

template <int schedulerId>
struct PrivateMeasurementConversionMetadata {
  explicit PrivateMeasurementConversionMetadata(
      const ConversionMetadata& conversion) {
    if (conversion.inputEncryption == common::InputEncryption::Plaintext) {
      convValue =
          SecConvValue<schedulerId>(conversion.convValue, common::PARTNER);
    } else {
      typename SecConvValue<schedulerId>::ExtractedInt extractedConvValue(
          conversion.convValue);
      convValue = SecConvValue<schedulerId>(std::move(extractedConvValue));
    }
  }

  SecConvValue<schedulerId> convValue;
};

} // namespace pcf2_aggregation
