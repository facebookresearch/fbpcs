/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include "fbpcs/data_processing/unified_data_process/adapter/AdapterFactory.h"
#include "fbpcs/data_processing/unified_data_process/data_processor/DataProcessorFactory.h"
#include "fbpcs/emp_games/lift/metadata_compaction/IMetadataCompactorGame.h"
#include "fbpcs/emp_games/lift/pcf2_calculator/input_processing/CompactionBasedInputProcessor.h"

namespace private_lift {

template <int schedulerId>
class MetadataCompactorGame : public IMetadataCompactorGame<schedulerId>,
                              public fbpcf::frontend::MpcGame<schedulerId> {
 public:
  MetadataCompactorGame<schedulerId>(
      const int party,
      std::unique_ptr<fbpcf::scheduler::IScheduler> scheduler,
      fbpcf::engine::communication::IPartyCommunicationAgentFactory&
          agentFactory)
      : fbpcf::frontend::MpcGame<schedulerId>(std::move(scheduler)),
        party_{party},
        agentFactory_{agentFactory} {}

  std::unique_ptr<IInputProcessor<schedulerId>> play(
      InputData inputData,
      int32_t numConversionPerUser) override {
    int partnerParty =
        party_ == common::PUBLISHER ? common::PARTNER : common::PUBLISHER;
    auto adapter = unified_data_process::adapter::
                       getAdapterFactoryWithAsWaksmanBasedShuffler<schedulerId>(
                           party_ == common::PUBLISHER, party_, partnerParty)
                           ->create();
    auto dataProcessor =
        unified_data_process::data_processor::getDataProcessorFactoryWithAesCtr<
            schedulerId>(party_, partnerParty, agentFactory_)
            ->create();
    auto prg = std::make_unique<fbpcf::engine::util::AesPrgFactory>()->create(
        fbpcf::engine::util::getRandomM128iFromSystemNoise());

    return std::make_unique<CompactionBasedInputProcessor<schedulerId>>(
        party_,
        std::move(adapter),
        std::move(dataProcessor),
        std::move(prg),
        inputData,
        numConversionPerUser);
  }

 private:
  const int party_;
  fbpcf::engine::communication::IPartyCommunicationAgentFactory& agentFactory_;
};

} // namespace private_lift
