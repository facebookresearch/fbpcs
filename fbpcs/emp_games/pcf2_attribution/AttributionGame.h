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

template <int schedulerId>
struct MpcInputs {
  std::vector<std::vector<std::vector<SecTimestamp<schedulerId>>>>
      secTimeStamps_;
  std::vector<PrivateTouchpoint<schedulerId>> touchPoints_;
  std::vector<PrivateConversion<schedulerId>> conversions_;
  std::vector<std::shared_ptr<const AttributionRule<schedulerId>>> attrRules_;
  std::vector<int64_t> ids_;
};

template <int schedulerId>
class AttributionGame : public fbpcf::frontend::MpcGame<schedulerId> {
 public:
  explicit AttributionGame(
      std::unique_ptr<fbpcf::scheduler::IScheduler> scheduler)
      : fbpcf::frontend::MpcGame<schedulerId>(std::move(scheduler)) {}

  AttributionOutputMetrics computeAttributions(
      const int myRole,
      const AttributionInputMetrics& inputData,
      common::InputEncryption inputEncryption);

  using PrivateTouchpointT = PrivateTouchpoint<schedulerId>;
  using PrivateConversionT = PrivateConversion<schedulerId>;

  MpcInputs<schedulerId> prepareMpcInputs(
      const int myRole,
      const AttributionInputMetrics& inputData,
      common::InputEncryption inputEncryption);

  AttributionOutputMetrics computeAttributions_impl(
      std::vector<std::vector<std::vector<SecTimestamp<schedulerId>>>>&
          thresholdArraysForEachRule,
      std::vector<PrivateTouchpointT>& tpArrays,
      std::vector<PrivateConversionT>& convArrays,
      std::vector<std::shared_ptr<const AttributionRule<schedulerId>>>&
          attributionRules,
      std::vector<int64_t>& ids);

  /**
   * Publisher shares attribution rules with partner.
   */
  std::vector<std::shared_ptr<const AttributionRule<schedulerId>>>
  shareAttributionRules(
      const int myRole,
      const std::vector<std::string>& attributionRuleNames);

  /**
   * Publisher shares touchpoints with partner.
   */
  std::vector<PrivateTouchpointT> privatelyShareTouchpoints(
      const std::vector<Touchpoint>& touchpoints,
      common::InputEncryption inputEncryption);

  /**
   * Partner shares conversions with publisher.
   */
  std::vector<PrivateConversionT> privatelyShareConversions(
      const std::vector<Conversion>& conversions,
      common::InputEncryption inputEncryption);

  /**
   * Publisher shares touchpoints thresholds, to optimize attribution
   * computation.
   */
  std::vector<std::vector<SecTimestamp<schedulerId>>> privatelyShareThresholds(
      const std::vector<Touchpoint>& touchpoints,
      const std::vector<PrivateTouchpointT>& privateTouchpoints,
      const AttributionRule<schedulerId>& attributionRule,
      size_t batchSize,
      common::InputEncryption inputEncryption);

  /**
   * Retrieve the original Ad Ids from touchpoint data
   */
  const std::vector<uint64_t> retrieveValidOriginalAdIds(
      const int myRole,
      std::vector<Touchpoint>& touchpoints,
      common::InputEncryption inputEncryption);
  /**
   * Create a compression map of the original Ad Id with the compressed Ad ID
   */

  void replaceAdIdWithCompressedAdId(
      std::vector<Touchpoint>& touchpoints,
      std::vector<uint64_t>& validOriginalAdIds);

  void putAdIdMappingJson(
      const CompressedAdIdToOriginalAdId& maps,
      std::string outputPath);

  /**
   * Helper method for computing attributions.
   */
  const std::vector<SecBit<schedulerId>> computeAttributionsHelper(
      const std::vector<PrivateTouchpoint<schedulerId>>& touchpoints,
      const std::vector<PrivateConversion<schedulerId>>& conversions,
      const AttributionRule<schedulerId>& attributionRule,
      const std::vector<std::vector<SecTimestamp<schedulerId>>>& thresholds,
      size_t batchSize);

  const std::vector<AttributionReformattedOutputFmt<schedulerId>>
  computeAttributionsHelperV2(
      const std::vector<PrivateTouchpoint<schedulerId>>& touchpoints,
      const std::vector<PrivateConversion<schedulerId>>& conversions,
      const AttributionRule<schedulerId>& attributionRule,
      const std::vector<std::vector<SecTimestamp<schedulerId>>>& thresholds,
      size_t batchSize);
};

} // namespace pcf2_attribution

#include "fbpcs/emp_games/pcf2_attribution/AttributionGame_impl.h"
