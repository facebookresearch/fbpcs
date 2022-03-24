/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

/* @generated file - do not modify directly! */

#pragma once

#include <map>
#include <memory>
#include <string>

#include <fbpcf/engine/communication/SocketPartyCommunicationAgentFactory.h>
#include <fbpcf/frontend/mpcGame.h>
#include <fbpcf/mpc_std_lib/oram/IWriteOnlyOram.h>
#include <fbpcf/mpc_std_lib/oram/LinearOramFactory.h>
#include <fbpcf/scheduler/IScheduler.h>
#include <fbpcf/scheduler/SchedulerHelper.h>

namespace kodiak_cpp {

using CppMPCBool = typename fbpcf::frontend::MpcGame<0>::template SecBit<false>;
using CppMPCInt32 =
    typename fbpcf::frontend::MpcGame<0>::template SecSignedInt<32, false>;
using CppMPCInt64 =
    typename fbpcf::frontend::MpcGame<0>::template SecSignedInt<64, false>;
using CppMPCUInt32 =
    typename fbpcf::frontend::MpcGame<0>::template SecUnsignedInt<32, false>;
using CppMPCUInt64 =
    typename fbpcf::frontend::MpcGame<0>::template SecUnsignedInt<64, false>;
constexpr int32_t PUBLISHER_ROLE = 0;
constexpr int32_t PARTNER_ROLE = 1;

template <int schedulerId, bool batched = false>
class KodiakGameDetail : public fbpcf::frontend::MpcGame<schedulerId> {
 public:
  explicit KodiakGameDetail(
      std::unique_ptr<fbpcf::scheduler::IScheduler> scheduler)
      : fbpcf::frontend::MpcGame<schedulerId>(std::move(scheduler)) {}
};
class KodiakGame : public KodiakGameDetail<0> {
 public:
  explicit KodiakGame(std::unique_ptr<fbpcf::scheduler::IScheduler> scheduler)
      : KodiakGameDetail<0>(std::move(scheduler)) {}
};
std::unique_ptr<KodiakGame>
new_kodiak_game(int32_t role, const std::string& host, int16_t port) {
  std::map<
      int,
      fbpcf::engine::communication::SocketPartyCommunicationAgentFactory::
          PartyInfo>
      partyInfos{
          {{PUBLISHER_ROLE, {host, port}}, {PARTNER_ROLE, {host, port}}}};
  auto commAgentFactory = std::make_unique<
      fbpcf::engine::communication::SocketPartyCommunicationAgentFactory>(
      role, std::move(partyInfos));
  auto scheduler = fbpcf::scheduler::createLazySchedulerWithRealEngine(
      role, *commAgentFactory);
  return std::make_unique<KodiakGame>(std::move(scheduler));
}

std::unique_ptr<CppMPCBool> new_mpc_bool(bool a, int32_t partyId);
bool reveal_mpc_bool(const CppMPCBool& a);
std::unique_ptr<CppMPCBool> mpc_bool_and(
    const CppMPCBool& a,
    const CppMPCBool& b);
std::unique_ptr<CppMPCBool> mpc_bool_or(
    const CppMPCBool& a,
    const CppMPCBool& b);
std::unique_ptr<CppMPCBool> mpc_bool_xor(
    const CppMPCBool& a,
    const CppMPCBool& b);
std::unique_ptr<CppMPCInt32> new_mpc_int32(int32_t a, int32_t partyId);
int32_t reveal_mpc_int32(const CppMPCInt32& a);
std::unique_ptr<CppMPCInt32> mpc_int32_add(
    const CppMPCInt32& a,
    const CppMPCInt32& b);
std::unique_ptr<CppMPCInt32> mpc_int32_sub(
    const CppMPCInt32& a,
    const CppMPCInt32& b);
std::unique_ptr<CppMPCBool> mpc_int32_eq(
    const CppMPCInt32& a,
    const CppMPCInt32& b);
std::unique_ptr<CppMPCBool> mpc_int32_lt(
    const CppMPCInt32& a,
    const CppMPCInt32& b);
std::unique_ptr<CppMPCBool> mpc_int32_gt(
    const CppMPCInt32& a,
    const CppMPCInt32& b);
std::unique_ptr<CppMPCBool> mpc_int32_lte(
    const CppMPCInt32& a,
    const CppMPCInt32& b);
std::unique_ptr<CppMPCBool> mpc_int32_gte(
    const CppMPCInt32& a,
    const CppMPCInt32& b);
std::unique_ptr<CppMPCInt64> new_mpc_int64(int64_t a, int32_t partyId);
int64_t reveal_mpc_int64(const CppMPCInt64& a);
std::unique_ptr<CppMPCInt64> mpc_int64_add(
    const CppMPCInt64& a,
    const CppMPCInt64& b);
std::unique_ptr<CppMPCInt64> mpc_int64_sub(
    const CppMPCInt64& a,
    const CppMPCInt64& b);
std::unique_ptr<CppMPCBool> mpc_int64_eq(
    const CppMPCInt64& a,
    const CppMPCInt64& b);
std::unique_ptr<CppMPCBool> mpc_int64_lt(
    const CppMPCInt64& a,
    const CppMPCInt64& b);
std::unique_ptr<CppMPCBool> mpc_int64_gt(
    const CppMPCInt64& a,
    const CppMPCInt64& b);
std::unique_ptr<CppMPCBool> mpc_int64_lte(
    const CppMPCInt64& a,
    const CppMPCInt64& b);
std::unique_ptr<CppMPCBool> mpc_int64_gte(
    const CppMPCInt64& a,
    const CppMPCInt64& b);
std::unique_ptr<CppMPCUInt32> new_mpc_uint32(uint32_t a, int32_t partyId);
uint32_t reveal_mpc_uint32(const CppMPCUInt32& a);
std::unique_ptr<CppMPCUInt32> mpc_uint32_add(
    const CppMPCUInt32& a,
    const CppMPCUInt32& b);
std::unique_ptr<CppMPCUInt32> mpc_uint32_sub(
    const CppMPCUInt32& a,
    const CppMPCUInt32& b);
std::unique_ptr<CppMPCBool> mpc_uint32_eq(
    const CppMPCUInt32& a,
    const CppMPCUInt32& b);
std::unique_ptr<CppMPCBool> mpc_uint32_lt(
    const CppMPCUInt32& a,
    const CppMPCUInt32& b);
std::unique_ptr<CppMPCBool> mpc_uint32_gt(
    const CppMPCUInt32& a,
    const CppMPCUInt32& b);
std::unique_ptr<CppMPCBool> mpc_uint32_lte(
    const CppMPCUInt32& a,
    const CppMPCUInt32& b);
std::unique_ptr<CppMPCBool> mpc_uint32_gte(
    const CppMPCUInt32& a,
    const CppMPCUInt32& b);
std::unique_ptr<CppMPCUInt64> new_mpc_uint64(uint64_t a, int32_t partyId);
uint64_t reveal_mpc_uint64(const CppMPCUInt64& a);
std::unique_ptr<CppMPCUInt64> mpc_uint64_add(
    const CppMPCUInt64& a,
    const CppMPCUInt64& b);
std::unique_ptr<CppMPCUInt64> mpc_uint64_sub(
    const CppMPCUInt64& a,
    const CppMPCUInt64& b);
std::unique_ptr<CppMPCBool> mpc_uint64_eq(
    const CppMPCUInt64& a,
    const CppMPCUInt64& b);
std::unique_ptr<CppMPCBool> mpc_uint64_lt(
    const CppMPCUInt64& a,
    const CppMPCUInt64& b);
std::unique_ptr<CppMPCBool> mpc_uint64_gt(
    const CppMPCUInt64& a,
    const CppMPCUInt64& b);
std::unique_ptr<CppMPCBool> mpc_uint64_lte(
    const CppMPCUInt64& a,
    const CppMPCUInt64& b);
std::unique_ptr<CppMPCBool> mpc_uint64_gte(
    const CppMPCUInt64& a,
    const CppMPCUInt64& b);
} // namespace kodiak_cpp
