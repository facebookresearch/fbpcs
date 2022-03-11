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

struct TouchpointMetadata {
  uint64_t originalAdId;
  uint64_t ts;
  bool isClick;
  uint64_t campaignMetadata;
  uint16_t adId;

  /**
   * If both are clicks, or both are views, the earliest one comes first.
   * If one is a click but the other is a view, the view comes first.
   */
  bool operator<(const TouchpointMetadata& tpm) const {
    return (isClick == tpm.isClick) ? (ts < tpm.ts) : !isClick;
  }
};

template <int schedulerId>
struct PrivateMeasurementTouchpointMetadata {
  explicit PrivateMeasurementTouchpointMetadata(
      const TouchpointMetadata& touchpoint)
      : adId(touchpoint.adId, common::PUBLISHER) {}

  explicit PrivateMeasurementTouchpointMetadata(
      const SecAdId<schedulerId>& secAdId)
      : adId(secAdId) {}

  SecAdId<schedulerId> adId;
};

} // namespace pcf2_aggregation
