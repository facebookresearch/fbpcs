/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#ifndef SECRET_SHARING_H
#define SECRET_SHARING_H

#include <functional>
#include <optional>
#include <tuple>
#include <vector>

#include <emp-sh2pc/emp-sh2pc.h>

#include "PrivateData.h"

namespace private_measurement::secret_sharing {

/*
 * Share one emp::Integer bidirectionally between both parties
 */
template <int MY_ROLE>
PrivateInt<MY_ROLE> privatelyShareInt(int64_t in);

/*
 * Share emp::Integers from SOURCE_ROLE to the opposite party
 * numVals = number of items to share
 */
template <int MY_ROLE, int SOURCE_ROLE>
const std::vector<emp::Integer> privatelyShareIntsFrom(
    const std::vector<int64_t>& in,
    int64_t numVals,
    int32_t bitLen = INT_SIZE);

/*
 * Share emp::Bits from SOURCE_ROLE to the opposite party
 * numVals = number of items to share
 */
template <int MY_ROLE, int SOURCE_ROLE>
const std::vector<emp::Bit> privatelyShareBitsFrom(
    const std::vector<int64_t>& in,
    int64_t numVals);

/*
 * Share an array of type T from SOURCE_ROLE to the opposite party,
 * return an array of type O.
 *
 * O must be emp::batcher compatible. That means O must implement
 *  1) O::bool_size(T val)
 *  2) O::bool_data(bool* data, T val)
 *  3) O(int32_t len, const emp::block* b)
 *
 * T must also be ostream and == compatible to support debug logging.
 *
 * numVals = number of items to share
 * nullValue = value to initialize for the non-source role.
 */
template <int MY_ROLE, int SOURCE_ROLE, typename T, typename O>
const std::vector<O>
privatelyShareArrayFrom(const std::vector<T>& in, int64_t numVals, T nullValue);

/*
 * Share an array of T arrays from SOURCE_ROLE to the opposite party,
 * returning a vector of O arrays.
 *
 * The inner arrays will be padded to prevent the other party from
 * learning how many items are in the arrays.
 *
 * privatelyShareArrayFrom will be used to share the inner arrays.
 *
 * maxArraySize = maximum inner array size
 * paddingValue = value to pad the inner arrays with
 * numVals = number of items to share
 */
template <int MY_ROLE, int SOURCE_ROLE, typename T, typename O>
const std::vector<std::vector<O>> privatelyShareArraysFrom(
    const std::vector<std::vector<T>>& in,
    int64_t numVals,
    int64_t maxArraySize,
    T paddingValue);

/*
 * Share an array of pre-padded int arrays from SOURCE_ROLE to the opposite
 * party.
 *
 * The inner arrays must be in size arraySize. No padding will be performed.
 *
 * privatelyShareArrayFrom will be used to share the inner arrays.
 *
 * numVals = number of items to share
 * arraySize = mandatory inner array size
 * bitLen = number of bits each emp::integer takes to optimize memory usage
 */
template <int MY_ROLE, int SOURCE_ROLE>
const std::vector<std::vector<emp::Integer>>
privatelyShareIntArraysNoPaddingFrom(
    const std::vector<std::vector<int64_t>>& in,
    int64_t numVals,
    int64_t arraySize,
    int32_t bitLen);

/*
 * Share emp::Integers from ALICE to BOB
 * numVals = number of items to share
 */
template <int MY_ROLE>
const std::vector<emp::Integer> privatelyShareIntsFromAlice(
    const std::vector<int64_t>& in,
    int64_t numVals,
    int32_t bitLen = INT_SIZE) {
  return privatelyShareIntsFrom<MY_ROLE, emp::ALICE>(in, numVals, bitLen);
}

/*
 * Share emp::Integers from BOB to ALICE
 * numVals = number of items to share
 */
template <int MY_ROLE>
const std::vector<emp::Integer> privatelyShareIntsFromBob(
    const std::vector<int64_t>& in,
    int64_t numVals,
    int32_t bitLen = INT_SIZE) {
  return privatelyShareIntsFrom<MY_ROLE, emp::BOB>(in, numVals, bitLen);
}

/*
 * Share emp::Bits from ALICE to BOB
 * numVals = number of items to share
 */
template <int MY_ROLE>
const std::vector<emp::Bit> privatelyShareBitsFromAlice(
    const std::vector<int64_t>& in,
    int64_t numVals) {
  return privatelyShareBitsFrom<MY_ROLE, emp::ALICE>(in, numVals);
}

/*
 * Share emp::Bits from BOB to ALICE
 * numVals = number of items to share
 */
template <int MY_ROLE>
const std::vector<emp::Bit> privatelyShareBitsFromBob(
    const std::vector<int64_t>& in,
    int64_t numVals) {
  return privatelyShareBitsFrom<MY_ROLE, emp::BOB>(in, numVals);
}

/*
 * Share an array of arrays from ALICE to BOB
 *
 * The inner arrays will be padded to prevent the other party from
 * learning how many items are in the arrays.
 *
 * maxArraySize = maximum inner array size
 * paddingValue = value to pad the inner arrays with
 * numVals = number of items to share
 */
template <int MY_ROLE, typename T, typename O>
const std::vector<std::vector<O>> privatelyShareArraysFromAlice(
    const std::vector<std::vector<T>>& in,
    int64_t numVals,
    int64_t maxArraySize,
    T paddingValue) {
  return privatelyShareArraysFrom<MY_ROLE, emp::ALICE, T, O>(
      in, numVals, maxArraySize, paddingValue);
}

/*
 * Share an array of arrays from BOB to ALICE
 *
 * The inner arrays will be padded to prevent the other party from
 * learning how many items are in the arrays.
 *
 * maxArraySize = maximum inner array size
 * paddingValue = value to pad the inner arrays with
 * numVals = number of items to share
 */
template <int MY_ROLE, typename T, typename O>
const std::vector<std::vector<O>> privatelyShareArraysFromBob(
    const std::vector<std::vector<T>>& in,
    int64_t numVals,
    int64_t maxArraySize,
    T paddingValue) {
  return privatelyShareArraysFrom<MY_ROLE, emp::BOB, T, O>(
      in, numVals, maxArraySize, paddingValue);
}

/*
 * Share an array of pre-padded int arrays from BOB to ALICE
 *
 * The inner arrays must be in size arraySize. No padding will be performed.
 *
 * arraySize = mandatory inner array size
 * numVals = number of items to share
 * bitLen = number of bits each emp::integer takes to optimize memory usage
 */
template <int MY_ROLE>
const std::vector<std::vector<emp::Integer>>
privatelyShareIntArraysNoPaddingFromBob(
    const std::vector<std::vector<int64_t>>& in,
    int64_t numVals,
    int64_t arraySize,
    int32_t bitLen) {
  return privatelyShareIntArraysNoPaddingFrom<MY_ROLE, emp::BOB>(
      in, numVals, arraySize, bitLen);
}

/*
 * Execute map_fn on pairwise items from vec1 and vec2
 */
template <typename T, typename S>
void zip(
    const std::vector<T>& vec1,
    const std::vector<S>& vec2,
    std::function<void(T, S)> map_fn);

/*
 * Execute map_fn on elements of vec and returns mapped values
 * construct a vector of the return type of map_fn and return that to
 * the caller
 */
template <typename T, typename O>
const std::vector<O> map(const std::vector<T>& vec, std::function<O(T)> map_fn);

/*
 * Execute map_fn on pairwise items from vec1 and vec2
 * construct a vector of the return type of map_fn and return that to
 * the caller
 */
template <typename T, typename S, typename O>
const std::vector<O> zip_and_map(
    const std::vector<T>& vec1,
    const std::vector<S>& vec2,
    std::function<O(T, S)> map_fn);

/*
 * Execute map_fn on pairwise items from vec1 and vec2
 * construct a vector of the return type of map_fn and return that to
 * the caller.
 */
template <typename T, typename S, typename O, typename N>
const std::pair<std::vector<O>, std::vector<N>> zip_and_map(
    const std::vector<T>& vec1,
    const std::vector<S>& vec2,
    std::function<std::pair<O, N>(T, S)> map_fn);

template <typename T, typename S, typename O1, typename O2, typename O3>
const std::tuple<std::vector<O1>, std::vector<O2>, std::vector<O3>> zip_and_map(
    const std::vector<T>& vec1,
    const std::vector<S>& vec2,
    std::function<std::tuple<O1, O2, O3>(T, S)> map_fn);

/*
 * Execute map_fn on pairwise items from vec1, vec2, and vec3
 * construct a vector of the return type of map_fn and return that to
 * the caller
 */
template <typename T, typename S, typename R, typename O>
const std::vector<O> zip_and_map(
    const std::vector<T>& vec1,
    const std::vector<S>& vec2,
    const std::vector<R>& vec3,
    std::function<O(T, S, R)> map_fn);

/*
 * Multiply vec by the bitmask. If the mask is 1 at element i, accept
 * vec[i] If the mask is 0 at element i, accept 0 (default constructed
 * T, effectively)
 */
template <typename T>
const std::vector<T> multiplyBitmask(
    const std::vector<T>& vec,
    const std::vector<emp::Bit>& bitmask);

} // namespace private_measurement::secret_sharing

#ifndef SECRET_SHARING_HPP
#include "SecretSharing.hpp"
#endif // SECRET_SHARING_HPP

#endif // SECRET_SHARING_H
