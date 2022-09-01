/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <cstdint>
#include <vector>
#include "fbpcs/emp_games/lift/pcf2_calculator/Constants.h"

namespace private_lift {

template <int schedulerId>
struct LiftGameProcessedData {
  int64_t numRows;
  uint32_t numPartnerCohorts;
  uint32_t numPublisherBreakdowns;
  uint32_t numGroups;
  uint32_t numTestGroups;
  uint8_t valueBits;
  uint8_t valueSquaredBits;
  std::vector<std::vector<bool>> indexShares;
  std::vector<std::vector<bool>> testIndexShares;
  SecTimestamp<schedulerId> opportunityTimestamps;
  SecBit<schedulerId> isValidOpportunityTimestamp;
  std::vector<SecTimestamp<schedulerId>> purchaseTimestamps;
  std::vector<SecTimestamp<schedulerId>> thresholdTimestamps;
  SecBit<schedulerId> anyValidPurchaseTimestamp;
  std::vector<SecValue<schedulerId>> purchaseValues;
  std::vector<SecValueSquared<schedulerId>> purchaseValueSquared;
  SecBit<schedulerId> testReach;
};

} // namespace private_lift
