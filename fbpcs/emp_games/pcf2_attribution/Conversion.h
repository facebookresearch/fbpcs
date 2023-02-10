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
  SecTimestamp<schedulerId> ts;
  SecTargetId<schedulerId> targetId;
  SecActionType<schedulerId> actionType;
  SecConvValue<schedulerId> convValue;

  explicit PrivateConversion(const Conversion& conversion) {
    if constexpr (inputEncryption == common::InputEncryption::Plaintext) {
      ts = SecTimestamp<schedulerId>(conversion.ts, common::PARTNER);
      targetId = SecTargetId<schedulerId>(conversion.targetId, common::PARTNER);
      actionType =
          SecActionType<schedulerId>(conversion.actionType, common::PARTNER);
      convValue =
          SecConvValue<schedulerId>(conversion.convValue, common::PARTNER);
    } else {
      typename SecTimestamp<schedulerId>::ExtractedInt extractedTs(
          conversion.ts);
      ts = SecTimestamp<schedulerId>(std::move(extractedTs));
      typename SecTargetId<schedulerId>::ExtractedInt extractedTids(
          conversion.targetId);
      targetId = SecTargetId<schedulerId>(std::move(extractedTids));
      typename SecActionType<schedulerId>::ExtractedInt extractedAids(
          conversion.actionType);
      actionType = SecActionType<schedulerId>(std::move(extractedAids));
      typename SecConvValue<schedulerId>::ExtractedInt extractedVs(
          conversion.convValue);
      convValue = SecConvValue<schedulerId>(std::move(extractedVs));
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
