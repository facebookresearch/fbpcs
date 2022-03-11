/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <gtest/gtest_prod.h>
#include "folly/logging/xlog.h"

#include "fbpcf/frontend/mpcGame.h"
#include "fbpcs/emp_games/common/Debug.h"
#include "fbpcs/emp_games/common/Util.h"
#include "fbpcs/emp_games/pcf2_attribution/AttributionMetrics.h"
#include "fbpcs/emp_games/pcf2_attribution/AttributionOptions.h"
#include "fbpcs/emp_games/pcf2_attribution/AttributionRule.h"
#include "fbpcs/emp_games/pcf2_attribution/Constants.h"
#include "fbpcs/emp_games/pcf2_attribution/Conversion.h"
#include "fbpcs/emp_games/pcf2_attribution/Touchpoint.h"

namespace pcf2_attribution {

template <
    int schedulerId,
    bool usingBatch,
    common::InputEncryption inputEncryption>
class AttributionGame : public fbpcf::frontend::MpcGame<schedulerId> {
 public:
  explicit AttributionGame(
      std::unique_ptr<fbpcf::scheduler::IScheduler> scheduler)
      : fbpcf::frontend::MpcGame<schedulerId>(std::move(scheduler)) {}

  AttributionOutputMetrics computeAttributions(
      const int myRole,
      const AttributionInputMetrics<usingBatch, inputEncryption>& inputData);

 private:
  using PrivateTouchpointT = ConditionalVector<
      PrivateTouchpoint<schedulerId, usingBatch, inputEncryption>,
      !usingBatch>;

  using PrivateConversionT = ConditionalVector<
      PrivateConversion<schedulerId, usingBatch, inputEncryption>,
      !usingBatch>;

  /**
   * Publisher shares attribution rules with partner.
   */
  const std::vector<AttributionRule<schedulerId, usingBatch, inputEncryption>>
  shareAttributionRules(
      const int myRole,
      const std::vector<std::string>& attributionRuleNames);

  /**
   * Publisher shares touchpoints with partner.
   */
  std::vector<PrivateTouchpointT> privatelyShareTouchpoints(
      const std::vector<TouchpointT<usingBatch>>& touchpoints);

  /**
   * Partner shares conversions with publisher.
   */
  std::vector<PrivateConversionT> privatelyShareConversions(
      const std::vector<ConversionT<usingBatch>>& conversions);

  /**
   * Publisher shares touchpoints thresholds, to optimize attribution
   * computation.
   */
  std::vector<std::vector<SecTimestampT<schedulerId, usingBatch>>>
  privatelyShareThresholds(
      const std::vector<TouchpointT<usingBatch>>& touchpoints,
      const std::vector<PrivateTouchpointT>& privateTouchpoints,
      const AttributionRule<schedulerId, usingBatch, inputEncryption>&
          attributionRule,
      size_t batchSize);

  /**
   * Helper method for computing attributions.
   */
  const std::vector<SecBit<schedulerId, usingBatch>> computeAttributionsHelper(
      const std::vector<
          PrivateTouchpoint<schedulerId, usingBatch, inputEncryption>>&
          touchpoints,
      const std::vector<
          PrivateConversion<schedulerId, usingBatch, inputEncryption>>&
          conversions,
      const AttributionRule<schedulerId, usingBatch, inputEncryption>&
          attributionRule,
      const std::vector<std::vector<SecTimestamp<schedulerId, usingBatch>>>&
          thresholds,
      size_t batchSize);

  FRIEND_TEST(AttributionGameTest, TestPrivateTouchpointPlaintext);
  FRIEND_TEST(AttributionGameTest, TestPrivateTouchpointPlaintextBatch);
  FRIEND_TEST(AttributionGameTest, TestPrivateConversionPlaintext);
  FRIEND_TEST(AttributionGameTest, TestPrivateConversionPlaintextBatch);
  FRIEND_TEST(AttributionGameTest, TestAttributionLogicPlaintext);
  FRIEND_TEST(AttributionGameTest, TestAttributionLogicPlaintextBatch);
  FRIEND_TEST(AttributionGameTest, TestShareAttributionRules);
};

} // namespace pcf2_attribution

#include "fbpcs/emp_games/pcf2_attribution/AttributionGame_impl.h"
