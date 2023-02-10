/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include "fbpcs/emp_games/common/Constants.h"
#include "fbpcs/emp_games/pcf2_attribution/Constants.h"

namespace pcf2_attribution {

struct Conversion {
  std::vector<uint64_t> ts;
  std::vector<uint64_t> targetId;
  std::vector<uint64_t> actionType;
  std::vector<uint64_t> convValue;
};

template <int schedulerId, common::InputEncryption inputEncryption>
struct PrivateConversion {
  SecTimestamp<schedulerId, true> ts;
  SecTargetId<schedulerId, true> targetId;
  SecActionType<schedulerId, true> actionType;
  SecConvValue<schedulerId, true> convValue;

  explicit PrivateConversion(const Conversion& conversion) {
    if constexpr (inputEncryption == common::InputEncryption::Plaintext) {
      ts = SecTimestamp<schedulerId, true>(conversion.ts, common::PARTNER);
      targetId =
          SecTargetId<schedulerId, true>(conversion.targetId, common::PARTNER);
      actionType = SecActionType<schedulerId, true>(
          conversion.actionType, common::PARTNER);
      convValue = SecConvValue<schedulerId, true>(
          conversion.convValue, common::PARTNER);
    } else {
      typename SecTimestamp<schedulerId, true>::ExtractedInt extractedTs(
          conversion.ts);
      ts = SecTimestamp<schedulerId, true>(std::move(extractedTs));
      typename SecTargetId<schedulerId, true>::ExtractedInt extractedTids(
          conversion.targetId);
      targetId = SecTargetId<schedulerId, true>(std::move(extractedTids));
      typename SecActionType<schedulerId, true>::ExtractedInt extractedAids(
          conversion.actionType);
      actionType = SecActionType<schedulerId, true>(std::move(extractedAids));
      typename SecConvValue<schedulerId, true>::ExtractedInt extractedVs(
          conversion.convValue);
      convValue = SecConvValue<schedulerId, true>(std::move(extractedVs));
    }
  }
};

// Used for parsing conversions from input CSV files
struct ParsedConversion {
  uint64_t ts = 0U;
  uint64_t targetId = 0U;
  uint64_t actionType = 0U;
  uint64_t convValue = 0U;

  bool operator<(const ParsedConversion& conv) const {
    return (ts < conv.ts);
  }
};

} // namespace pcf2_attribution
