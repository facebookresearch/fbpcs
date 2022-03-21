/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include "fbpcs/data_processing/unified_data_process/adapter/Adapter.h"
#include "fbpcs/data_processing/unified_data_process/adapter/IAdapterFactory.h"

namespace unified_data_process::adapter {

template <int schedulerId>
class AdapterFactory final : public IAdapterFactory {
  using SecString = fbpcf::frontend::BitString<true, schedulerId, true>;

 public:
  AdapterFactory(
      bool amIParty0,
      int32_t party0Id,
      int32_t party1Id,
      std::unique_ptr<fbpcf::mpc_std_lib::shuffler::IShufflerFactory<SecString>>
          shufflerFactory)
      : amIParty0_(amIParty0),
        party0Id_(party0Id),
        party1Id_(party1Id),
        shufflerFactory_(std::move(shufflerFactory)) {}
  std::unique_ptr<IAdapter> create() override {
    return std::make_unique<Adapter<schedulerId>>(
        amIParty0_, party0Id_, party1Id_, shufflerFactory_->create());
  }

 private:
  bool amIParty0_;
  int32_t party0Id_;
  int32_t party1Id_;
  std::unique_ptr<fbpcf::mpc_std_lib::shuffler::IShufflerFactory<SecString>>
      shufflerFactory_;
};

} // namespace unified_data_process::adapter
