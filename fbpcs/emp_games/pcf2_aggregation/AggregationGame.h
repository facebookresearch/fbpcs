/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include "folly/logging/xlog.h"

#include "fbpcf/engine/communication/IPartyCommunicationAgent.h"
#include "fbpcf/engine/communication/IPartyCommunicationAgentFactory.h"
#include "fbpcf/frontend/mpcGame.h"
#include "fbpcs/emp_games/common/Constants.h"
#include "fbpcs/emp_games/common/Util.h"
#include "fbpcs/emp_games/pcf2_aggregation/AggregationMetrics.h"
#include "fbpcs/emp_games/pcf2_aggregation/AggregationOptions.h"
#include "fbpcs/emp_games/pcf2_aggregation/Aggregator.h"
#include "fbpcs/emp_games/pcf2_aggregation/AttributionResult.h"
#include "fbpcs/emp_games/pcf2_aggregation/Constants.h"
#include "fbpcs/emp_games/pcf2_aggregation/ConversionMetadata.h"
#include "fbpcs/emp_games/pcf2_aggregation/TouchpointMetadata.h"

namespace pcf2_aggregation {

template <int schedulerId>
class AggregationGame : public fbpcf::frontend::MpcGame<schedulerId> {
 public:
  explicit AggregationGame(
      std::unique_ptr<fbpcf::scheduler::IScheduler> scheduler,
      std::shared_ptr<
          fbpcf::engine::communication::IPartyCommunicationAgentFactory>
          communicationAgentFactory,
      common::InputEncryption inputEncryption,
      const int concurrency = 1)
      : fbpcf::frontend::MpcGame<schedulerId>(std::move(scheduler)),
        communicationAgentFactory_(communicationAgentFactory),
        inputEncryption_(inputEncryption),
        concurrency_(concurrency) {}

  /**
   * Publisher shares aggregation formats with partner
   */
  const std::vector<AggregationFormat<schedulerId>> shareAggregationFormats(
      const int myRole,
      const std::vector<std::string>& aggregationFormatNames);

  /**
   * Publisher privately shares measurement touchpoint metadata with partner.
   */
  std::vector<std::vector<PrivateMeasurementTouchpointMetadata<schedulerId>>>
  privatelyShareMeasurementTouchpointMetadata(
      const std::vector<std::vector<TouchpointMetadata>>& touchpointMetadata);

  /**
   * Partner privately shares measurement conversion metadata with publisher.
   */
  std::vector<std::vector<PrivateMeasurementConversionMetadata<schedulerId>>>
  privatelyShareMeasurementConversionMetadata(
      const std::vector<std::vector<ConversionMetadata>>& conversionMetadata);

  /**
   * Both parties read attribution results as secret shared bits.
   */
  std::vector<std::vector<PrivateAttributionResult<schedulerId>>>
  privatelyShareAttributionResults(
      const std::vector<std::vector<AttributionResult>>& attributionResults);

  std::vector<std::vector<PrivateAttributionReformattedResult<schedulerId>>>
  privatelyShareAttributionReformattedResults(
      const std::vector<std::vector<AttributionReformattedResult>>&
          attributionReformattedResults);

  /**
   * Both parties share and retrieve valid original ad ids.
   */
  const std::vector<uint64_t> retrieveValidOriginalAdIds(
      const int myRole,
      std::vector<std::vector<TouchpointMetadata>>& touchpointMetadataArrays);

  /** Ad Ids are represented by 64 bit integers. For measurement aggregation
   * computation, the number of ad Ids received is much smaller. Thus for the
   * computation, we are mapping original adId to compressed adId. This method
   * will map the adIds to compressed adIds, replacing all original ad Ids with
   * compressed values in touchpoint Metadata.
   */
  void replaceAdIdWithCompressedAdId(
      std::vector<std::vector<TouchpointMetadata>>& touchpointMetadataArrays,
      std::vector<uint64_t>& validOriginalAdIds);

  AggregationOutputMetrics computeAggregations(
      const int myRole,
      const AggregationInputMetrics& inputData);

  AggregationOutputMetrics computeAggregationsReformatted(
      const int myRole,
      const AggregationInputMetrics& inputData);

 private:
  std::shared_ptr<fbpcf::engine::communication::IPartyCommunicationAgentFactory>
      communicationAgentFactory_;
  common::InputEncryption inputEncryption_;
  const int concurrency_;
};

} // namespace pcf2_aggregation

#include "fbpcs/emp_games/pcf2_aggregation/AggregationGame_impl.h"
