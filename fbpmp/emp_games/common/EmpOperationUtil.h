/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <numeric>
#include <vector>

#include <emp-sh2pc/emp-sh2pc.h>

namespace private_measurement::emp_utils {

// Converts a vector of emp::Integers to emp::Bits.
// We only take the first (zero-th) bit. It's up to the caller to ensure
// that the input vector of Integers actually represents Bits.
const std::vector<emp::Bit> intsToBits(const std::vector<emp::Integer>& in);

// Converts a vector of emp::Bits to emp::Integers since we can't add Bits
// TODO: Could probably be smarter about INT_SIZE here given the size of the
// input dataset -- if we have N values, output can't be larger than log(N)
const std::vector<emp::Integer> bitsToInts(const std::vector<emp::Bit>& in);

// Sums the given vector of integers and then reveals the result
// Supports 32 bit and 64 bit input integers
template <int TO = emp::PUBLIC>
const int64_t sum(const std::vector<emp::Integer>& in);

// Sums the given vector of bits, as if they were integers.
template <int TO = emp::PUBLIC>
const int64_t sum(const std::vector<emp::Bit>& in);

// Sum operations that do *not* call reveal at the end
emp::Integer secretSum(const std::vector<emp::Integer>& in);
emp::Integer secretSum(const std::vector<emp::Bit>& in);

// Computes and returns the minimum between two emp::Integer values
const emp::Integer getMin(emp::Integer value1, emp::Integer value2);

// Computes and returns the minimum from an emp::Integer vector
const emp::Integer getMin(const std::vector<emp::Integer>& values);

// Returns emp::Bit true if the predicate evaluates to true for any of the bit
// in the input vector
template <typename T>
emp::Bit any(const std::vector<T>& in, std::function<emp::Bit(T)> predicate);

// Returns emp::Bit true if the predicate evaluates to true for all of the bit
// in the input vector
template <typename T>
emp::Bit all(const std::vector<T>& in, std::function<emp::Bit(T)> predicate);

// Returns emp::Bit true if any of the emp::Bit is true
// in the input vector
emp::Bit any(const std::vector<emp::Bit>& in);

// Returns emp::Bit true if all of the emp::Bits are true
// in the input vector
emp::Bit all(const std::vector<emp::Bit>& in);
} // namespace private_measurement::emp_utils

#include "EmpOperationUtil.hpp"
