/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include "fbpcf/engine/communication/IPartyCommunicationAgent.h"
#include "fbpcs/data_processing/unified_data_process/data_processor/IDataProcessor.h"

namespace unified_data_process::data_processor::insecure {

/**
 * This is an insecure implementation. This object is only meant to be used as a
 * placeholder for testing.
 */
template <int schedulerId>
class DummyDataProcessor final : public IDataProcessor<schedulerId> {
 public:
  explicit DummyDataProcessor(
      int32_t myId,
      int32_t partnerId,
      std::unique_ptr<fbpcf::engine::communication::IPartyCommunicationAgent>
          agent)
      : myId_(myId), partnerId_(partnerId), agent_(std::move(agent)) {}

  /**
   * @inherit doc
   */
  typename IDataProcessor<schedulerId>::SecString processMyData(
      const std::vector<std::vector<unsigned char>>& plaintextData,
      size_t outputSize) override;

  /**
   * @inherit doc
   */
  typename IDataProcessor<schedulerId>::SecString processPeersData(
      size_t dataSize,
      const std::vector<int64_t>& indexes,
      size_t dataWidth) override;

 private:
  int32_t myId_;
  int32_t partnerId_;
  std::unique_ptr<fbpcf::engine::communication::IPartyCommunicationAgent>
      agent_;
};

} // namespace unified_data_process::data_processor::insecure

#include "fbpcs/data_processing/unified_data_process/data_processor/DummyDataProcessor_impl.h"
