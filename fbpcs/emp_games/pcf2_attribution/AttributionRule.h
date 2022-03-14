/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <stdexcept>

#include <fmt/format.h>

#include "fbpcs/emp_games/pcf2_attribution/Conversion.h"
#include "fbpcs/emp_games/pcf2_attribution/Touchpoint.h"

namespace pcf2_attribution {

const uint32_t kSecondsInOneDay = 86400; // 60 * 60 * 24
const uint32_t kSecondsInTwentyEightDays = 2419200; // 60 * 60 * 24 * 28
const uint32_t kSecondsInSevenDays = 604800; // 60 * 60 * 24 * 7

template <
    int schedulerId,
    bool usingBatch,
    common::InputEncryption inputEncryption>
struct AttributionRule {
  // Integer that should uniquely identify this attribution rule. Used
  // to synchronize between the publisher and partner
  const uint64_t id;

  // Human readable name for the this attribution rule. The publisher will
  // pass in a list of names, and the output json will be keyed by names
  const std::string name;

  // Should return true if the given touchpoint is eligible to be attributed
  // to the given conversion
  const std::function<const SecBit<schedulerId, usingBatch>(
      const PrivateTouchpoint<schedulerId, usingBatch, inputEncryption>&,
      const PrivateConversion<schedulerId, usingBatch, inputEncryption>&,
      const std::vector<SecTimestamp<schedulerId, usingBatch>>&)>
      isAttributable;

  // Compute touchpoint thresholds from plaintext touchpoints based on
  // attribution rule
  const std::function<const std::vector<SecTimestamp<schedulerId, usingBatch>>(
      const Touchpoint<usingBatch>&)>
      computeThresholdsPlaintext;

  // Compute touchpoint thresholds from private touchpoints based on attribution
  // rule
  const std::function<const std::vector<SecTimestamp<schedulerId, usingBatch>>(
      const PrivateTouchpoint<schedulerId, usingBatch, inputEncryption>&,
      const PrivateIsClick<schedulerId, usingBatch, inputEncryption>&,
      size_t batchSize)>
      computeThresholdsPrivate;

  // Constructors for attribution rules, which can be found in
  // AttributionRule.cpp
  static const AttributionRule fromNameOrThrow(const std::string& name);
  static const AttributionRule fromIdOrThrow(int64_t id);
};

} // namespace pcf2_attribution

#include "fbpcs/emp_games/pcf2_attribution/AttributionRule_impl.h"
