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

template <bool usingBatch>
struct Conversion {
  ConditionalVector<uint64_t, usingBatch> ts;
  ConditionalVector<uint64_t, usingBatch> targetId;
  ConditionalVector<uint64_t, usingBatch> actionType;
  ConditionalVector<uint64_t, usingBatch> convValue;
};

template <bool usingBatch>
using ConversionT = ConditionalVector<Conversion<usingBatch>, !usingBatch>;

template <
    int schedulerId,
    bool usingBatch,
    common::InputEncryption inputEncryption>
struct PrivateConversion {
  SecTimestamp<schedulerId, usingBatch> ts;
  SecTargetId<schedulerId, usingBatch> targetId;
  SecActionType<schedulerId, usingBatch> actionType;
  SecConvValue<schedulerId, usingBatch> convValue;

  explicit PrivateConversion(const Conversion<usingBatch>& conversion) {
    if constexpr (inputEncryption == common::InputEncryption::Plaintext) {
      ts =
          SecTimestamp<schedulerId, usingBatch>(conversion.ts, common::PARTNER);
      targetId = SecTargetId<schedulerId, usingBatch>(
          conversion.targetId, common::PARTNER);
      actionType = SecActionType<schedulerId, usingBatch>(
          conversion.actionType, common::PARTNER);
      convValue = SecConvValue<schedulerId, usingBatch>(
          conversion.convValue, common::PARTNER);
    } else {
      typename SecTimestamp<schedulerId, usingBatch>::ExtractedInt extractedTs(
          conversion.ts);
      ts = SecTimestamp<schedulerId, usingBatch>(std::move(extractedTs));
      typename SecTargetId<schedulerId, usingBatch>::ExtractedInt extractedTids(
          conversion.targetId);
      targetId = SecTargetId<schedulerId, usingBatch>(std::move(extractedTids));
      typename SecActionType<schedulerId, usingBatch>::ExtractedInt
          extractedAids(conversion.actionType);
      actionType =
          SecActionType<schedulerId, usingBatch>(std::move(extractedAids));
      typename SecConvValue<schedulerId, usingBatch>::ExtractedInt extractedVs(
          conversion.convValue);
      convValue = SecConvValue<schedulerId, usingBatch>(std::move(extractedVs));
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
