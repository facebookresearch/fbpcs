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

template <int schedulerId, common::InputEncryption inputEncryption>
struct PrivateTouchpoint {
  std::vector<int64_t> id;
  SecTimestamp<schedulerId> ts;
  SecTargetId<schedulerId> targetId;
  SecActionType<schedulerId> actionType;
  SecOriginalAdId<schedulerId> originalAdId;
  SecAdId<schedulerId> adId;

  explicit PrivateTouchpoint(const Touchpoint& touchpoint) : id{touchpoint.id} {
    if constexpr (inputEncryption == common::InputEncryption::Xor) {
      typename SecTimestamp<schedulerId>::ExtractedInt extractedTs(
          touchpoint.ts);
      ts = SecTimestamp<schedulerId>(std::move(extractedTs));
      typename SecTargetId<schedulerId>::ExtractedInt extractedTids(
          touchpoint.targetId);
      targetId = SecTargetId<schedulerId>(std::move(extractedTids));
      typename SecActionType<schedulerId>::ExtractedInt extractedAids(
          touchpoint.actionType);
      actionType = SecActionType<schedulerId>(std::move(extractedAids));
      typename SecOriginalAdId<schedulerId>::ExtractedInt
          extractedOriginalAdIds(touchpoint.originalAdId);
      originalAdId =
          SecOriginalAdId<schedulerId>(std::move(extractedOriginalAdIds));
    } else {
      ts = SecTimestamp<schedulerId>(touchpoint.ts, common::PUBLISHER);
      targetId =
          SecTargetId<schedulerId>(touchpoint.targetId, common::PUBLISHER);
      actionType =
          SecActionType<schedulerId>(touchpoint.actionType, common::PUBLISHER);
      originalAdId = SecOriginalAdId<schedulerId>(
          touchpoint.originalAdId, common::PUBLISHER);
    }
    adId = SecAdId<schedulerId>(touchpoint.adId, common::PUBLISHER);
  }
};

// Used for privately sharing isClick for xor encrypted inputs
template <int schedulerId, common::InputEncryption inputEncryption>
struct PrivateIsClick {
  SecBit<schedulerId> isClick;

  explicit PrivateIsClick(const Touchpoint& touchpoint) {
    if constexpr (inputEncryption == common::InputEncryption::Xor) {
      typename SecBit<schedulerId>::ExtractedBit extractedIsClick(
          touchpoint.isClick);
      isClick = SecBit<schedulerId>(std::move(extractedIsClick));
    } else {
      isClick = SecBit<schedulerId>(touchpoint.isClick, common::PUBLISHER);
    }
  }
};

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
