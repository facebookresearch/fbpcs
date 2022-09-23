/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <memory>
#include "fbpcf/engine/communication/IPartyCommunicationAgentFactory.h"
#include "fbpcf/mpc_std_lib/aes_circuit/AesCircuitCtrFactory.h"
#include "fbpcs/data_processing/unified_data_process/data_processor/DataProcessor.h"
#include "fbpcs/data_processing/unified_data_process/data_processor/IDataProcessor.h"
#include "fbpcs/data_processing/unified_data_process/data_processor/IDataProcessorFactory.h"

namespace unified_data_process::data_processor {

template <int schedulerId>
class DataProcessorFactory final : public IDataProcessorFactory<schedulerId> {
 public:
  using AesCtrFactory = fbpcf::mpc_std_lib::aes_circuit::AesCircuitCtrFactory<
      typename IDataProcessor<schedulerId>::SecBit>;

  DataProcessorFactory(
      int32_t myId,
      int32_t partnerId,
      fbpcf::engine::communication::IPartyCommunicationAgentFactory&
          agentFactory,
      std::unique_ptr<AesCtrFactory> aesCtrFactory)
      : myId_(myId),
        partnerId_(partnerId),
        agentFactory_(agentFactory),
        aesCtrFactory_(std::move(aesCtrFactory)) {}

  std::unique_ptr<IDataProcessor<schedulerId>> create() {
    return std::make_unique<DataProcessor<schedulerId>>(
        myId_,
        partnerId_,
        agentFactory_.create(partnerId_, "data_processor_traffic"),
        aesCtrFactory_->create());
  }

 private:
  int32_t myId_;
  int32_t partnerId_;
  fbpcf::engine::communication::IPartyCommunicationAgentFactory& agentFactory_;
  std::unique_ptr<AesCtrFactory> aesCtrFactory_;
};

} // namespace unified_data_process::data_processor
