/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include "fbpcf/frontend/BitString.h"
#include "fbpcf/mpc_std_lib/shuffler/IShuffler.h"
#include "fbpcs/data_processing/unified_data_process/adapter/IAdapter.h"

namespace unified_data_process::adapter {

template <int schedulerId>
class Adapter final : public IAdapter {
  using SecBit = fbpcf::frontend::Bit<true, schedulerId, true>;
  using PubBit = fbpcf::frontend::Bit<false, schedulerId, true>;
  using SecString = fbpcf::frontend::BitString<true, schedulerId, true>;

 public:
  Adapter(
      bool amIParty0,
      int32_t party0Id,
      int32_t party1Id,
      std::unique_ptr<fbpcf::mpc_std_lib::shuffler::IShuffler<SecString>>
          shuffler)
      : amIParty0_(amIParty0),
        party0Id_(party0Id),
        party1Id_(party1Id),
        shuffler_(std::move(shuffler)) {}

  std::vector<int64_t> adapt(
      const std::vector<int64_t>& unionMap) const override;

 private:
  bool amIParty0_;
  int32_t party0Id_;
  int32_t party1Id_;
  std::unique_ptr<fbpcf::mpc_std_lib::shuffler::IShuffler<SecString>> shuffler_;
};

} // namespace unified_data_process::adapter

#include "fbpcs/data_processing/unified_data_process/adapter/Adapter_impl.h"
