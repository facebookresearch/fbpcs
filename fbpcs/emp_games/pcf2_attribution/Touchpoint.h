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

struct Touchpoint {
  std::vector<int64_t> id;
  std::vector<bool> isClick;
  std::vector<uint64_t> ts;
  std::vector<uint64_t> targetId;
  std::vector<uint64_t> actionType;
  std::vector<uint64_t> originalAdId;
  std::vector<uint64_t> adId;
};

template <int schedulerId>
struct PrivateTouchpoint {
  std::vector<int64_t> id;
  SecTimestamp<schedulerId> ts;
  SecTargetId<schedulerId> targetId;
  SecActionType<schedulerId> actionType;
  SecOriginalAdId<schedulerId> originalAdId;
  SecAdId<schedulerId> adId;
};

template <int schedulerId>
PrivateTouchpoint<schedulerId> createPrivateTouchpoint(
    common::InputEncryption inputEncryption,
    const Touchpoint& touchpoint) {
  PrivateTouchpoint<schedulerId> rst{.id = touchpoint.id};
  if (inputEncryption == common::InputEncryption::Xor) {
    typename SecTimestamp<schedulerId>::ExtractedInt extractedTs(touchpoint.ts);
    rst.ts = SecTimestamp<schedulerId>(std::move(extractedTs));
    typename SecTargetId<schedulerId>::ExtractedInt extractedTids(
        touchpoint.targetId);
    rst.targetId = SecTargetId<schedulerId>(std::move(extractedTids));
    typename SecActionType<schedulerId>::ExtractedInt extractedAids(
        touchpoint.actionType);
    rst.actionType = SecActionType<schedulerId>(std::move(extractedAids));
    typename SecOriginalAdId<schedulerId>::ExtractedInt extractedOriginalAdIds(
        touchpoint.originalAdId);
    rst.originalAdId =
        SecOriginalAdId<schedulerId>(std::move(extractedOriginalAdIds));
  } else {
    rst.ts = SecTimestamp<schedulerId>(touchpoint.ts, common::PUBLISHER);
    rst.targetId =
        SecTargetId<schedulerId>(touchpoint.targetId, common::PUBLISHER);
    rst.actionType =
        SecActionType<schedulerId>(touchpoint.actionType, common::PUBLISHER);
    rst.originalAdId = SecOriginalAdId<schedulerId>(
        touchpoint.originalAdId, common::PUBLISHER);
  }
  rst.adId = SecAdId<schedulerId>(touchpoint.adId, common::PUBLISHER);
  return rst;
}

// Used for privately sharing isClick for xor encrypted inputs
template <int schedulerId>
struct PrivateIsClick {
  SecBit<schedulerId> isClick;
};

template <int schedulerId>
PrivateIsClick<schedulerId> createPrivateIsClick(
    common::InputEncryption inputEncryption,
    const Touchpoint& touchpoint) {
  PrivateIsClick<schedulerId> rst;
  if (inputEncryption == common::InputEncryption::Xor) {
    typename SecBit<schedulerId>::ExtractedBit extractedIsClick(
        touchpoint.isClick);
    rst.isClick = SecBit<schedulerId>(std::move(extractedIsClick));
  } else {
    rst.isClick = SecBit<schedulerId>(touchpoint.isClick, common::PUBLISHER);
  }
  return rst;
}

// Used for parsing touchpoints from input CSV files
struct ParsedTouchpoint {
  int64_t id = -1;
  bool isClick = false;
  uint64_t ts = 0U;
  uint64_t targetId = 0U;
  uint64_t actionType = 0U;
  uint64_t originalAdId = 0U;
  uint16_t adId = 0U;

  /**
   * If both are clicks, or both are views, the earliest one comes first.
   * If one is a click but the other is a view, the view comes first.
   */
  bool operator<(const ParsedTouchpoint& tp) const {
    return (isClick == tp.isClick) ? (ts < tp.ts) : !isClick;
  }
};

} // namespace pcf2_attribution
