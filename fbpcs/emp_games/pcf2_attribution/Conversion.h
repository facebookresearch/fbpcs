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

template <int schedulerId>
struct PrivateConversion {
  SecTimestamp<schedulerId> ts;
  SecTargetId<schedulerId> targetId;
  SecActionType<schedulerId> actionType;
  SecConvValue<schedulerId> convValue;
};

template <int schedulerId>
PrivateConversion<schedulerId> createPrivateConversion(
    common::InputEncryption inputEncryption,
    const Conversion& conversion) {
  PrivateConversion<schedulerId> rst;
  if (inputEncryption == common::InputEncryption::Plaintext) {
    rst.ts = SecTimestamp<schedulerId>(conversion.ts, common::PARTNER);
    rst.targetId =
        SecTargetId<schedulerId>(conversion.targetId, common::PARTNER);
    rst.actionType =
        SecActionType<schedulerId>(conversion.actionType, common::PARTNER);
    rst.convValue =
        SecConvValue<schedulerId>(conversion.convValue, common::PARTNER);
  } else {
    typename SecTimestamp<schedulerId>::ExtractedInt extractedTs(conversion.ts);
    rst.ts = SecTimestamp<schedulerId>(std::move(extractedTs));
    typename SecTargetId<schedulerId>::ExtractedInt extractedTids(
        conversion.targetId);
    rst.targetId = SecTargetId<schedulerId>(std::move(extractedTids));
    typename SecActionType<schedulerId>::ExtractedInt extractedAids(
        conversion.actionType);
    rst.actionType = SecActionType<schedulerId>(std::move(extractedAids));
    typename SecConvValue<schedulerId>::ExtractedInt extractedVs(
        conversion.convValue);
    rst.convValue = SecConvValue<schedulerId>(std::move(extractedVs));
  }
  return rst;
}

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
