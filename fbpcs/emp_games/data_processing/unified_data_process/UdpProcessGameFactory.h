/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <memory>
#include "fbpcf/engine/util/AesPrgFactory.h"
#include "fbpcf/mpc_std_lib/permuter/AsWaksmanPermuterFactory.h"
#include "fbpcf/mpc_std_lib/permuter/IPermuterFactory.h"
#include "fbpcf/mpc_std_lib/shuffler/IShuffler.h"
#include "fbpcf/mpc_std_lib/shuffler/IShufflerFactory.h"
#include "fbpcf/mpc_std_lib/shuffler/PermuteBasedShufflerFactory.h"
#include "fbpcf/mpc_std_lib/unified_data_process/adapter/AdapterFactory.h"
#include "fbpcf/mpc_std_lib/unified_data_process/data_processor/DataProcessorFactory.h"
#include "fbpcs/emp_games/data_processing/unified_data_process/UdpProcessGame.h"

namespace unified_data_process {

template <int schedulerId>
class UdpProcessGameFactory {
 public:
  explicit UdpProcessGameFactory(
      int partyId,
      fbpcf::engine::communication::IPartyCommunicationAgentFactory&
          communicationAgentFactory)
      : partyId_(partyId),
        communicationAgentFactory_(communicationAgentFactory) {}

  std::unique_ptr<UdpProcessGame<schedulerId>> create(
      std::unique_ptr<fbpcf::scheduler::IScheduler> scheduler) {
    auto adapterFactory = std::make_unique<
        fbpcf::mpc_std_lib::unified_data_process::adapter::AdapterFactory<
            schedulerId>>(
        partyId_ == common::PUBLISHER,
        0,
        1,
        std::make_unique<
            fbpcf::mpc_std_lib::shuffler::PermuteBasedShufflerFactory<
                fbpcf::frontend::BitString<true, schedulerId, true>>>(
            partyId_,
            1 - partyId_,
            std::make_unique<
                fbpcf::mpc_std_lib::permuter::
                    AsWaksmanPermuterFactory<std::vector<bool>, schedulerId>>(
                partyId_, 1 - partyId_),
            std::make_unique<fbpcf::engine::util::AesPrgFactory>()));
    auto dataProcessorFactory = std::make_unique<
        fbpcf::mpc_std_lib::unified_data_process::data_processor::
            DataProcessorFactory<schedulerId>>(
        partyId_,
        1 - partyId_,
        communicationAgentFactory_,
        std::make_unique<fbpcf::mpc_std_lib::aes_circuit::AesCircuitCtrFactory<
            typename UdpProcessGame<schedulerId>::SecBit>>());
    return std::make_unique<UdpProcessGame<schedulerId>>(
        partyId_,
        std::move(scheduler),
        std::move(adapterFactory),
        std::move(dataProcessorFactory));
  }

 private:
  int partyId_;
  fbpcf::engine::communication::IPartyCommunicationAgentFactory&
      communicationAgentFactory_;
};

} // namespace unified_data_process
