/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include "fbpcf/frontend/mpcGame.h"
#include "fbpcs/emp_games/common/Debug.h"
#include "fbpcs/emp_games/common/Util.h"
#include "fbpcs/emp_games/pcf2_attribution/AttributionMetrics.h"
#include "fbpcs/emp_games/pcf2_attribution/AttributionOptions.h"
#include "fbpcs/emp_games/pcf2_attribution/AttributionRule.h"
#include "fbpcs/emp_games/pcf2_attribution/Constants.h"
#include "fbpcs/emp_games/pcf2_attribution/Conversion.h"
#include "fbpcs/emp_games/pcf2_attribution/Touchpoint.h"
#include "folly/logging/xlog.h"

namespace pcf2_attribution {

template <int schedulerId, common::InputEncryption inputEncryption>
class AttributionGame : public fbpcf::frontend::MpcGame<schedulerId> {
 public:
  explicit AttributionGame(
      std::unique_ptr<fbpcf::scheduler::IScheduler> scheduler)
      : fbpcf::frontend::MpcGame<schedulerId>(std::move(scheduler)) {}

  AttributionOutputMetrics computeAttributions(
      const int myRole,
      const AttributionInputMetrics<inputEncryption>& inputData);

  using PrivateTouchpointT =
      PrivateTouchpoint<schedulerId, true, inputEncryption>;

  using PrivateConversionT =
      PrivateConversion<schedulerId, true, inputEncryption>;

  /**
   * Publisher shares attribution rules with partner.
   */
  std::vector<std::shared_ptr<
      const AttributionRule<schedulerId, true, inputEncryption>>>
  shareAttributionRules(
      const int myRole,
      const std::vector<std::string>& attributionRuleNames);

  /**
   * Publisher shares touchpoints with partner.
   */
  std::vector<PrivateTouchpointT> privatelyShareTouchpoints(
      const std::vector<TouchpointT<true>>& touchpoints);

  /**
   * Partner shares conversions with publisher.
   */
  std::vector<PrivateConversionT> privatelyShareConversions(
      const std::vector<ConversionT<true>>& conversions);

  /**
   * Publisher shares touchpoints thresholds, to optimize attribution
   * computation.
   */
  std::vector<std::vector<SecTimestampT<schedulerId, true>>>
  privatelyShareThresholds(
      const std::vector<TouchpointT<true>>& touchpoints,
      const std::vector<PrivateTouchpointT>& privateTouchpoints,
      const AttributionRule<schedulerId, true, inputEncryption>&
          attributionRule,
      size_t batchSize);

  /**
   * Retrieve the original Ad Ids from touchpoint data
   */
  const std::vector<uint64_t> retrieveValidOriginalAdIds(
      const int myRole,
      std::vector<TouchpointT<true>>& touchpoints);
  /**
   * Create a compression map of the original Ad Id with the compressed Ad ID
   */

  void replaceAdIdWithCompressedAdId(
      std::vector<TouchpointT<true>>& touchpoints,
      std::vector<uint64_t>& validOriginalAdIds);

  void putAdIdMappingJson(
      const CompressedAdIdToOriginalAdId& maps,
      std::string outputPath);

  /**
   * Helper method for computing attributions.
   */
  const std::vector<SecBit<schedulerId, true>> computeAttributionsHelper(
      const std::vector<PrivateTouchpoint<schedulerId, true, inputEncryption>>&
          touchpoints,
      const std::vector<PrivateConversion<schedulerId, true, inputEncryption>>&
          conversions,
      const AttributionRule<schedulerId, true, inputEncryption>&
          attributionRule,
      const std::vector<std::vector<SecTimestamp<schedulerId, true>>>&
          thresholds,
      size_t batchSize);

  const std::vector<AttributionReformattedOutputFmt<schedulerId>>
  computeAttributionsHelperV2(
      const std::vector<PrivateTouchpoint<schedulerId, true, inputEncryption>>&
          touchpoints,
      const std::vector<PrivateConversion<schedulerId, true, inputEncryption>>&
          conversions,
      const AttributionRule<schedulerId, true, inputEncryption>&
          attributionRule,
      const std::vector<std::vector<SecTimestamp<schedulerId, true>>>&
          thresholds,
      size_t batchSize);
};

} // namespace pcf2_attribution

#include "fbpcs/emp_games/pcf2_attribution/AttributionGame_impl.h"
