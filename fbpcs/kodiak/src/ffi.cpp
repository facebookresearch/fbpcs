/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

/* @generated file - do not modify directly! */

#include "fbpcs/kodiak/include/ffi.h"

#include <memory>

using namespace kodiak_cpp;

std::unique_ptr<CppMPCBool> new_mpc_bool(bool a, int32_t partyId) {
  return std::make_unique<CppMPCBool>(a, partyId);
}
bool reveal_mpc_bool(const CppMPCBool& a) {
  auto res = a.openToParty(0);
  return res.getValue();
}
std::unique_ptr<CppMPCBool> mpc_bool_and(
    const CppMPCBool& a,
    const CppMPCBool& b) {
  return std::make_unique<CppMPCBool>(a & b);
}
std::unique_ptr<CppMPCBool> mpc_bool_or(
    const CppMPCBool& a,
    const CppMPCBool& b) {
  return std::make_unique<CppMPCBool>(a || b);
}
std::unique_ptr<CppMPCBool> mpc_bool_xor(
    const CppMPCBool& a,
    const CppMPCBool& b) {
  return std::make_unique<CppMPCBool>(a ^ b);
}
std::unique_ptr<CppMPCInt32> new_mpc_int32(int32_t a, int32_t partyId) {
  return std::make_unique<CppMPCInt32>(a, partyId);
}
int32_t reveal_mpc_int32(const CppMPCInt32& a) {
  auto res = a.openToParty(0);
  return res.getValue();
}
std::unique_ptr<CppMPCInt32> mpc_int32_add(
    const CppMPCInt32& a,
    const CppMPCInt32& b) {
  return std::make_unique<CppMPCInt32>(a + b);
}
std::unique_ptr<CppMPCInt32> mpc_int32_sub(
    const CppMPCInt32& a,
    const CppMPCInt32& b) {
  return std::make_unique<CppMPCInt32>(a - b);
}
std::unique_ptr<CppMPCBool> mpc_int32_eq(
    const CppMPCInt32& a,
    const CppMPCInt32& b) {
  return std::make_unique<CppMPCBool>(a == b);
}
std::unique_ptr<CppMPCBool> mpc_int32_lt(
    const CppMPCInt32& a,
    const CppMPCInt32& b) {
  return std::make_unique<CppMPCBool>(a < b);
}
std::unique_ptr<CppMPCBool> mpc_int32_gt(
    const CppMPCInt32& a,
    const CppMPCInt32& b) {
  return std::make_unique<CppMPCBool>(a > b);
}
std::unique_ptr<CppMPCBool> mpc_int32_lte(
    const CppMPCInt32& a,
    const CppMPCInt32& b) {
  return std::make_unique<CppMPCBool>(a <= b);
}
std::unique_ptr<CppMPCBool> mpc_int32_gte(
    const CppMPCInt32& a,
    const CppMPCInt32& b) {
  return std::make_unique<CppMPCBool>(a >= b);
}
std::unique_ptr<CppMPCInt64> new_mpc_int64(int64_t a, int32_t partyId) {
  return std::make_unique<CppMPCInt64>(a, partyId);
}
int64_t reveal_mpc_int64(const CppMPCInt64& a) {
  auto res = a.openToParty(0);
  return res.getValue();
}
std::unique_ptr<CppMPCInt64> mpc_int64_add(
    const CppMPCInt64& a,
    const CppMPCInt64& b) {
  return std::make_unique<CppMPCInt64>(a + b);
}
std::unique_ptr<CppMPCInt64> mpc_int64_sub(
    const CppMPCInt64& a,
    const CppMPCInt64& b) {
  return std::make_unique<CppMPCInt64>(a - b);
}
std::unique_ptr<CppMPCBool> mpc_int64_eq(
    const CppMPCInt64& a,
    const CppMPCInt64& b) {
  return std::make_unique<CppMPCBool>(a == b);
}
std::unique_ptr<CppMPCBool> mpc_int64_lt(
    const CppMPCInt64& a,
    const CppMPCInt64& b) {
  return std::make_unique<CppMPCBool>(a < b);
}
std::unique_ptr<CppMPCBool> mpc_int64_gt(
    const CppMPCInt64& a,
    const CppMPCInt64& b) {
  return std::make_unique<CppMPCBool>(a > b);
}
std::unique_ptr<CppMPCBool> mpc_int64_lte(
    const CppMPCInt64& a,
    const CppMPCInt64& b) {
  return std::make_unique<CppMPCBool>(a <= b);
}
std::unique_ptr<CppMPCBool> mpc_int64_gte(
    const CppMPCInt64& a,
    const CppMPCInt64& b) {
  return std::make_unique<CppMPCBool>(a >= b);
}
std::unique_ptr<CppMPCUInt32> new_mpc_uint32(uint32_t a, int32_t partyId) {
  return std::make_unique<CppMPCUInt32>(a, partyId);
}
uint32_t reveal_mpc_uint32(const CppMPCUInt32& a) {
  auto res = a.openToParty(0);
  return res.getValue();
}
std::unique_ptr<CppMPCUInt32> mpc_uint32_add(
    const CppMPCUInt32& a,
    const CppMPCUInt32& b) {
  return std::make_unique<CppMPCUInt32>(a + b);
}
std::unique_ptr<CppMPCUInt32> mpc_uint32_sub(
    const CppMPCUInt32& a,
    const CppMPCUInt32& b) {
  return std::make_unique<CppMPCUInt32>(a - b);
}
std::unique_ptr<CppMPCBool> mpc_uint32_eq(
    const CppMPCUInt32& a,
    const CppMPCUInt32& b) {
  return std::make_unique<CppMPCBool>(a == b);
}
std::unique_ptr<CppMPCBool> mpc_uint32_lt(
    const CppMPCUInt32& a,
    const CppMPCUInt32& b) {
  return std::make_unique<CppMPCBool>(a < b);
}
std::unique_ptr<CppMPCBool> mpc_uint32_gt(
    const CppMPCUInt32& a,
    const CppMPCUInt32& b) {
  return std::make_unique<CppMPCBool>(a > b);
}
std::unique_ptr<CppMPCBool> mpc_uint32_lte(
    const CppMPCUInt32& a,
    const CppMPCUInt32& b) {
  return std::make_unique<CppMPCBool>(a <= b);
}
std::unique_ptr<CppMPCBool> mpc_uint32_gte(
    const CppMPCUInt32& a,
    const CppMPCUInt32& b) {
  return std::make_unique<CppMPCBool>(a >= b);
}
std::unique_ptr<CppMPCUInt64> new_mpc_uint64(uint64_t a, int32_t partyId) {
  return std::make_unique<CppMPCUInt64>(a, partyId);
}
uint64_t reveal_mpc_uint64(const CppMPCUInt64& a) {
  auto res = a.openToParty(0);
  return res.getValue();
}
std::unique_ptr<CppMPCUInt64> mpc_uint64_add(
    const CppMPCUInt64& a,
    const CppMPCUInt64& b) {
  return std::make_unique<CppMPCUInt64>(a + b);
}
std::unique_ptr<CppMPCUInt64> mpc_uint64_sub(
    const CppMPCUInt64& a,
    const CppMPCUInt64& b) {
  return std::make_unique<CppMPCUInt64>(a - b);
}
std::unique_ptr<CppMPCBool> mpc_uint64_eq(
    const CppMPCUInt64& a,
    const CppMPCUInt64& b) {
  return std::make_unique<CppMPCBool>(a == b);
}
std::unique_ptr<CppMPCBool> mpc_uint64_lt(
    const CppMPCUInt64& a,
    const CppMPCUInt64& b) {
  return std::make_unique<CppMPCBool>(a < b);
}
std::unique_ptr<CppMPCBool> mpc_uint64_gt(
    const CppMPCUInt64& a,
    const CppMPCUInt64& b) {
  return std::make_unique<CppMPCBool>(a > b);
}
std::unique_ptr<CppMPCBool> mpc_uint64_lte(
    const CppMPCUInt64& a,
    const CppMPCUInt64& b) {
  return std::make_unique<CppMPCBool>(a <= b);
}
std::unique_ptr<CppMPCBool> mpc_uint64_gte(
    const CppMPCUInt64& a,
    const CppMPCUInt64& b) {
  return std::make_unique<CppMPCBool>(a >= b);
}
