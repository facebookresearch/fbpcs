/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <memory>

#include "fbpcf/frontend/mpcGame.h"
#include "fbpcs/data_processing/unified_data_process/adapter/IAdapterFactory.h"
#include "fbpcs/data_processing/unified_data_process/data_processor/IDataProcessorFactory.h"
#include "fbpcs/emp_games/common/Constants.h"
#include "fbpcs/emp_games/common/Util.h"

namespace unified_data_process {

template <int schedulerId>
class UdpProcessGame : public fbpcf::frontend::MpcGame<schedulerId> {
 public:
  using SecString = fbpcf::frontend::BitString<true, schedulerId, true>;
  using PubString = fbpcf::frontend::BitString<false, schedulerId, true>;
  using SecBit = fbpcf::frontend::Bit<true, schedulerId, true>;
  explicit UdpProcessGame(
      int32_t myId,
      std::unique_ptr<fbpcf::scheduler::IScheduler> scheduler,
      std::unique_ptr<unified_data_process::adapter::IAdapterFactory>
          adapterFactory,
      std::unique_ptr<
          unified_data_process::data_processor::IDataProcessorFactory<
              schedulerId>> dataProcessorFactory)
      : fbpcf::frontend::MpcGame<schedulerId>(std::move(scheduler)),
        myId_(myId),
        adapterFactory_(std::move(adapterFactory)),
        dataProcessorFactory_(std::move(dataProcessorFactory)) {}

  std::vector<int64_t> playAdapter(const std::vector<int64_t>& unionMap);
  std::tuple<std::vector<std::vector<bool>>, std::vector<std::vector<bool>>>
  playDataProcessor(
      const std::vector<std::vector<unsigned char>>& metaData,
      const std::vector<int64_t>& indexes,
      size_t peersDataSize,
      size_t peersDataWidth);

 private:
  int32_t myId_;
  std::unique_ptr<unified_data_process::adapter::IAdapterFactory>
      adapterFactory_;
  std::unique_ptr<
      unified_data_process::data_processor::IDataProcessorFactory<schedulerId>>
      dataProcessorFactory_;
};

} // namespace unified_data_process
