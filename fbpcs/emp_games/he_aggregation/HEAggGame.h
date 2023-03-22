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
#include "fbpcs/emp_games/he_aggregation/AggregationInputMetrics.h"
#include "fbpcs/emp_games/pcf2_aggregation/ConversionMetadata.h"
#include "fbpcs/emp_games/pcf2_aggregation/TouchpointMetadata.h"

#include "privacy_infra/elgamal/ElGamal.h"

namespace heschme = facebook::privacy_infra::elgamal;

namespace pcf2_he {

class HEAggGame {
 public:
  explicit HEAggGame(
      std::shared_ptr<
          fbpcf::engine::communication::IPartyCommunicationAgentFactory>
          communicationAgentFactory)
      : communicationAgentFactory_(communicationAgentFactory) {}

  std::unordered_map<uint64_t, uint64_t> computeAggregations(
      const int myRole,
      const AggregationInputMetrics& inputData);

 private:
  std::shared_ptr<fbpcf::engine::communication::IPartyCommunicationAgentFactory>
      communicationAgentFactory_;
};

} // namespace pcf2_he
