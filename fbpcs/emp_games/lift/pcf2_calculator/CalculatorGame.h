/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include "fbpcf/engine/communication/IPartyCommunicationAgentFactory.h"
#include "fbpcf/frontend/mpcGame.h"
#include "fbpcs/emp_games/common/Constants.h"
#include "fbpcs/emp_games/lift/pcf2_calculator/Aggregator.h"
#include "fbpcs/emp_games/lift/pcf2_calculator/Attributor.h"
#include "fbpcs/emp_games/lift/pcf2_calculator/CalculatorGameConfig.h"
#include "fbpcs/emp_games/lift/pcf2_calculator/InputData.h"
#include "fbpcs/emp_games/lift/pcf2_calculator/InputProcessor.h"

namespace private_lift {
template <int schedulerId>
class CalculatorGame : public fbpcf::frontend::MpcGame<schedulerId> {
 public:
  CalculatorGame(
      const int party,
      std::unique_ptr<fbpcf::scheduler::IScheduler> scheduler,
      std::shared_ptr<
          fbpcf::engine::communication::IPartyCommunicationAgentFactory>
          communicationAgentFactory)
      : fbpcf::frontend::MpcGame<schedulerId>(std::move(scheduler)),
        party_{party},
        communicationAgentFactory_(communicationAgentFactory) {}

  std::string play(const CalculatorGameConfig& config) {
    auto inputProcessor = InputProcessor<schedulerId>(
        party_, config.inputData, config.numConversionsPerUser);
    auto attributor =
        std::make_unique<Attributor<schedulerId>>(party_, inputProcessor);
    auto aggregator = Aggregator<schedulerId>(
        party_,
        inputProcessor,
        std::move(attributor),
        config.numConversionsPerUser,
        communicationAgentFactory_);
    return aggregator.toJson();
  }

 private:
  const int party_;
  std::shared_ptr<fbpcf::engine::communication::IPartyCommunicationAgentFactory>
      communicationAgentFactory_;
};
} // namespace private_lift
