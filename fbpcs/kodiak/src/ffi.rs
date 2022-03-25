/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#[cxx::bridge(namespace = kodiak_cpp)]
mod ffi {
    unsafe extern "C++" {
        // One or more headers with the matching C++ declarations. Our code
        // generators don't read it but it gets #include'd and used in static
        // assertions to ensure our picture of the FFI boundary is accurate.
        include!("fbpcs/kodiak/include/ffi.h");

        // Zero or more opaque types which both languages can pass around but
        // only C++ can see the fields.
        type KodiakGame;
        type CppMPCInt32;
        type CppMPCInt64;
        type CppMPCUInt32;
        type CppMPCUInt64;
        type CppMPCBool;

        // Functions implemented in C++.
        // Create a new game
        // NOTE: Construct a CxxString with cxx::let_cxx_string!()
        fn new_kodiak_game(role: i32, host: &CxxString, port: i16) -> UniquePtr<KodiakGame>;

        // Create new MPC types
        fn new_mpc_int32(value: i32, partyId: i32) -> UniquePtr<CppMPCInt32>;
        fn new_mpc_int64(value: i64, partyId: i32) -> UniquePtr<CppMPCInt64>;
        fn new_mpc_uint32(value: u32, partyId: i32) -> UniquePtr<CppMPCUInt32>;
        fn new_mpc_uint64(value: u64, partyId: i32) -> UniquePtr<CppMPCUInt64>;
        fn new_mpc_bool(value: bool, partyId: i32) -> UniquePtr<CppMPCBool>;

        // MPC Int32 functions
        fn mpc_int32_add(lhs: &CppMPCInt32, rhs: &CppMPCInt32) -> UniquePtr<CppMPCInt32>;
        fn mpc_int32_sub(lhs: &CppMPCInt32, rhs: &CppMPCInt32) -> UniquePtr<CppMPCInt32>;
        //fn mpc_int32_mul(lhs: &CppMPCInt32, rhs: &CppMPCInt32) -> UniquePtr<CppMPCInt32>;
        //fn mpc_int32_div(lhs: &CppMPCInt32, rhs: &CppMPCInt32) -> UniquePtr<CppMPCInt32>;
        //fn mpc_int32_and(lhs: &CppMPCInt32, rhs: &CppMPCInt32) -> UniquePtr<CppMPCInt32>;
        //fn mpc_int32_or(lhs: &CppMPCInt32, rhs: &CppMPCInt32) -> UniquePtr<CppMPCInt32>;
        //fn mpc_int32_xor(lhs: &CppMPCInt32, rhs: &CppMPCInt32) -> UniquePtr<CppMPCInt32>;
        fn mpc_int32_lt(lhs: &CppMPCInt32, rhs: &CppMPCInt32) -> UniquePtr<CppMPCBool>;
        fn mpc_int32_gt(lhs: &CppMPCInt32, rhs: &CppMPCInt32) -> UniquePtr<CppMPCBool>;
        fn mpc_int32_lte(lhs: &CppMPCInt32, rhs: &CppMPCInt32) -> UniquePtr<CppMPCBool>;
        fn mpc_int32_gte(lhs: &CppMPCInt32, rhs: &CppMPCInt32) -> UniquePtr<CppMPCBool>;
        fn mpc_int32_eq(lhs: &CppMPCInt32, rhs: &CppMPCInt32) -> UniquePtr<CppMPCBool>;
        //fn mpc_int32_neq(lhs: &CppMPCInt32, rhs: &CppMPCInt32) -> UniquePtr<CppMPCBool>;
        fn mpc_int32_mux(
            choice: &CppMPCBool,
            true_case: &CppMPCInt32,
            false_case: &CppMPCInt32,
        ) -> UniquePtr<CppMPCInt32>;

        // MPC Int64 functions
        fn mpc_int64_add(lhs: &CppMPCInt64, rhs: &CppMPCInt64) -> UniquePtr<CppMPCInt64>;
        fn mpc_int64_sub(lhs: &CppMPCInt64, rhs: &CppMPCInt64) -> UniquePtr<CppMPCInt64>;
        //fn mpc_int64_mul(lhs: &CppMPCInt64, rhs: &CppMPCInt64) -> UniquePtr<CppMPCInt64>;
        //fn mpc_int64_div(lhs: &CppMPCInt64, rhs: &CppMPCInt64) -> UniquePtr<CppMPCInt64>;
        //fn mpc_int64_and(lhs: &CppMPCInt64, rhs: &CppMPCInt64) -> UniquePtr<CppMPCInt64>;
        //fn mpc_int64_or(lhs: &CppMPCInt64, rhs: &CppMPCInt64) -> UniquePtr<CppMPCInt64>;
        //fn mpc_int64_xor(lhs: &CppMPCInt64, rhs: &CppMPCInt64) -> UniquePtr<CppMPCInt64>;
        fn mpc_int64_lt(lhs: &CppMPCInt64, rhs: &CppMPCInt64) -> UniquePtr<CppMPCBool>;
        fn mpc_int64_gt(lhs: &CppMPCInt64, rhs: &CppMPCInt64) -> UniquePtr<CppMPCBool>;
        fn mpc_int64_lte(lhs: &CppMPCInt64, rhs: &CppMPCInt64) -> UniquePtr<CppMPCBool>;
        fn mpc_int64_gte(lhs: &CppMPCInt64, rhs: &CppMPCInt64) -> UniquePtr<CppMPCBool>;
        fn mpc_int64_eq(lhs: &CppMPCInt64, rhs: &CppMPCInt64) -> UniquePtr<CppMPCBool>;
        //fn mpc_int64_neq(lhs: &CppMPCInt64, rhs: &CppMPCInt64) -> UniquePtr<CppMPCBool>;
        fn mpc_int64_mux(
            choice: &CppMPCBool,
            true_case: &CppMPCInt64,
            false_case: &CppMPCInt64,
        ) -> UniquePtr<CppMPCInt64>;

        // MPC UInt32 functions
        fn mpc_uint32_add(lhs: &CppMPCUInt32, rhs: &CppMPCUInt32) -> UniquePtr<CppMPCUInt32>;
        fn mpc_uint32_sub(lhs: &CppMPCUInt32, rhs: &CppMPCUInt32) -> UniquePtr<CppMPCUInt32>;
        //fn mpc_uint32_mul(lhs: &CppMPCUInt32, rhs: &CppMPCUInt32) -> UniquePtr<CppMPCUInt32>;
        //fn mpc_uint32_div(lhs: &CppMPCUInt32, rhs: &CppMPCUInt32) -> UniquePtr<CppMPCUInt32>;
        //fn mpc_uint32_and(lhs: &CppMPCUInt32, rhs: &CppMPCUInt32) -> UniquePtr<CppMPCUInt32>;
        //fn mpc_uint32_or(lhs: &CppMPCUInt32, rhs: &CppMPCUInt32) -> UniquePtr<CppMPCUInt32>;
        //fn mpc_uint32_xor(lhs: &CppMPCUInt32, rhs: &CppMPCUInt32) -> UniquePtr<CppMPCUInt32>;
        fn mpc_uint32_lt(lhs: &CppMPCUInt32, rhs: &CppMPCUInt32) -> UniquePtr<CppMPCBool>;
        fn mpc_uint32_gt(lhs: &CppMPCUInt32, rhs: &CppMPCUInt32) -> UniquePtr<CppMPCBool>;
        fn mpc_uint32_lte(lhs: &CppMPCUInt32, rhs: &CppMPCUInt32) -> UniquePtr<CppMPCBool>;
        fn mpc_uint32_gte(lhs: &CppMPCUInt32, rhs: &CppMPCUInt32) -> UniquePtr<CppMPCBool>;
        fn mpc_uint32_eq(lhs: &CppMPCUInt32, rhs: &CppMPCUInt32) -> UniquePtr<CppMPCBool>;
        //fn mpc_uint32_neq(lhs: &CppMPCUInt32, rhs: &CppMPCUInt32) -> UniquePtr<CppMPCBool>;
        fn mpc_uint32_mux(
            choice: &CppMPCBool,
            true_case: &CppMPCUInt32,
            false_case: &CppMPCUInt32,
        ) -> UniquePtr<CppMPCUInt32>;

        // MPC UInt64 functions
        fn mpc_uint64_add(lhs: &CppMPCUInt64, rhs: &CppMPCUInt64) -> UniquePtr<CppMPCUInt64>;
        fn mpc_uint64_sub(lhs: &CppMPCUInt64, rhs: &CppMPCUInt64) -> UniquePtr<CppMPCUInt64>;
        //fn mpc_uint64_mul(lhs: &CppMPCUInt64, rhs: &CppMPCUInt64) -> UniquePtr<CppMPCUInt64>;
        //fn mpc_uint64_div(lhs: &CppMPCUInt64, rhs: &CppMPCUInt64) -> UniquePtr<CppMPCUInt64>;
        //fn mpc_uint64_and(lhs: &CppMPCUInt64, rhs: &CppMPCUInt64) -> UniquePtr<CppMPCUInt64>;
        //fn mpc_uint64_or(lhs: &CppMPCUInt64, rhs: &CppMPCUInt64) -> UniquePtr<CppMPCUInt64>;
        //fn mpc_uint64_xor(lhs: &CppMPCUInt64, rhs: &CppMPCUInt64) -> UniquePtr<CppMPCUInt64>;
        fn mpc_uint64_lt(lhs: &CppMPCUInt64, rhs: &CppMPCUInt64) -> UniquePtr<CppMPCBool>;
        fn mpc_uint64_gt(lhs: &CppMPCUInt64, rhs: &CppMPCUInt64) -> UniquePtr<CppMPCBool>;
        fn mpc_uint64_lte(lhs: &CppMPCUInt64, rhs: &CppMPCUInt64) -> UniquePtr<CppMPCBool>;
        fn mpc_uint64_gte(lhs: &CppMPCUInt64, rhs: &CppMPCUInt64) -> UniquePtr<CppMPCBool>;
        fn mpc_uint64_eq(lhs: &CppMPCUInt64, rhs: &CppMPCUInt64) -> UniquePtr<CppMPCBool>;
        //fn mpc_uint64_neq(lhs: &CppMPCUInt64, rhs: &CppMPCUInt64) -> UniquePtr<CppMPCBool>;
        fn mpc_uint64_mux(
            choice: &CppMPCBool,
            true_case: &CppMPCUInt64,
            false_case: &CppMPCUInt64,
        ) -> UniquePtr<CppMPCUInt64>;

        // MPC bool functions
        fn mpc_bool_and(lhs: &CppMPCBool, rhs: &CppMPCBool) -> UniquePtr<CppMPCBool>;
        fn mpc_bool_or(lhs: &CppMPCBool, rhs: &CppMPCBool) -> UniquePtr<CppMPCBool>;
        fn mpc_bool_xor(lhs: &CppMPCBool, rhs: &CppMPCBool) -> UniquePtr<CppMPCBool>;
        //fn mpc_bool_eq(lhs: &CppMPCBool, rhs: &CppMPCBool) -> UniquePtr<CppMPCBool>;
        //fn mpc_bool_neq(lhs: &CppMPCBool, rhs: &CppMPCBool) -> UniquePtr<CppMPCBool>;

        // Reveal functions
        fn reveal_mpc_int32(val: &CppMPCInt32) -> i32;
        fn reveal_mpc_int64(val: &CppMPCInt64) -> i64;
        fn reveal_mpc_uint32(val: &CppMPCUInt32) -> u32;
        fn reveal_mpc_uint64(val: &CppMPCUInt64) -> u64;
        fn reveal_mpc_bool(val: &CppMPCBool) -> bool;
    }
}
