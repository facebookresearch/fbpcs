/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#ifndef SECRET_SHARING_HPP
#define SECRET_SHARING_HPP

#include <algorithm>
#include <functional>
#include <numeric>
#include <optional>
#include <stdexcept>
#include <tuple>
#include <vector>

#include "SecretSharing.h"
#include "folly/logging/xlog.h"

namespace private_measurement::secret_sharing {

template <int MY_ROLE>
PrivateInt<MY_ROLE> privatelyShareInt(int64_t in) {
  emp::Integer myInt{INT_SIZE, in, MY_ROLE};
  emp::Integer theirInt{INT_SIZE, in, otherRole(MY_ROLE)};

  return PrivateInt<MY_ROLE>{myInt, theirInt};
}

template <
    int MY_ROLE,
    int SOURCE_ROLE,
    typename T,
    typename O,
    typename... BatcherArgs>
const std::vector<O> privatelyShareArrayFrom(
    const std::vector<T>& in,
    int64_t numVals,
    T nullValue,
    BatcherArgs... batcherArgs) {
  const auto receiveStr = MY_ROLE == SOURCE_ROLE ? "sending" : "receiving";
  XLOGF(
      DBG,
      "Privately {} array[{}] = {}",
      receiveStr,
      numVals,
      privateVecToString<MY_ROLE, SOURCE_ROLE, T>(
          in, numVals, std::make_optional(nullValue)));
  emp::Batcher batcher;

  // Note that we must add an integer to both sides even though the data
  // transfer happens in one direction. This is so the underlying
  // library knows how much space to allocate. It's not exactly clear
  // from the API that this is a requirement.
  for (auto i = 0; i < numVals; ++i) {
    if constexpr (MY_ROLE == SOURCE_ROLE) {
      batcher.add<O>(std::forward<BatcherArgs>(batcherArgs)..., in.at(i));
    } else {
      T nullCopy = nullValue;
      batcher.add<O>(std::forward<BatcherArgs>(batcherArgs)..., nullCopy);
    }
  }

  batcher.make_semi_honest(SOURCE_ROLE);

  std::vector<O> out;
  out.reserve(numVals);
  for (auto i = 0; i < numVals; ++i) {
    out.push_back(batcher.next<O>());
  }

  return out;
}

// Some potential optimizations:
// 1) Rather than just padding to maxArraySize, use DP.
//    e.g Tell the other party to iterate max(C, rand(1, C)) for each
//    row
// 2) Send over the length of each row using log(bitLen) bits for each
// row 3) Limit the number of bits in the array of arrays, perhaps by
// taking the
//    delta from some minimum value (today's date for instance)
// 4) Reduce the number of elements passed in by combining "nearby"
// elements. 5) Enable compression at the socket level?
template <int MY_ROLE, int SOURCE_ROLE, typename T, typename O>
const std::vector<std::vector<O>> privatelyShareArraysFrom(
    const std::vector<std::vector<T>>& in,
    int64_t numVals,
    int64_t maxArraySize,
    T paddingValue) {
  const auto receiveStr = MY_ROLE == SOURCE_ROLE ? "sending" : "receiving";
  XLOGF(
      DBG,
      "Privately {} array[{}][max({})]",
      receiveStr,
      numVals,
      maxArraySize);

  // Pad the passed in arrays
  // POTENTIAL OPTIMIZATION: we don't need to store the padded arrays on
  // the publisher.
  std::vector<int64_t> paddedLengths;
  std::vector<std::vector<T>> paddedArrays;
  if (MY_ROLE == SOURCE_ROLE) {
    XLOG(DBG, "padding arrays");

    // POTENTIAL OPTIMIZATION: the value we reserve for the flattened
    // padded array is an upper bound. We can probably do better, if we
    // want to save memory.
    paddedLengths.reserve(numVals);
    paddedArrays.reserve(numVals * maxArraySize);

    for (auto i = 0; i < numVals; i++) {
      auto vec = in.at(i);
      auto arrayLength = vec.size();
      auto paddedLength = maxArraySize;

      if (arrayLength > maxArraySize) {
        throw std::runtime_error(fmt::format(
            "Input array {} of length {} is greater than allowed size {}",
            i,
            arrayLength,
            maxArraySize));
      }

      // Perform the padding
      std::vector<T> paddedVec(vec.begin(), vec.end());
      for (auto i = 0; i < paddedLength - arrayLength; i++) {
        const T paddingCopy = paddingValue;
        paddedVec.push_back(paddingCopy);
      }

      paddedArrays.push_back(paddedVec);
      paddedLengths.push_back(paddedLength);
    }
  } else {
    // Still allocate the outer arrays
    for (auto i = 0; i < numVals; i++) {
      paddedArrays.push_back(std::vector<T>());
    }
  }

  // Send over the lengths
  XLOGF(DBG, "{} padded array lengths", receiveStr);
  const auto empPaddedLengths = privatelyShareIntsFrom<MY_ROLE, SOURCE_ROLE>(
      paddedLengths, numVals, INT_SIZE);
  const auto revealedPaddedLengths = map<emp::Integer, int64_t>(
      empPaddedLengths,
      [](auto empLength) { return empLength.template reveal<int64_t>(); });

  // Send over the padded arrays
  XLOGF(DBG, "{} padded arrays", receiveStr);
  const auto out = zip_and_map<std::vector<T>, int64_t, std::vector<O>>(
      paddedArrays,
      revealedPaddedLengths,
      std::bind(
          privatelyShareArrayFrom<MY_ROLE, SOURCE_ROLE, T, O>,
          std::placeholders::_1,
          std::placeholders::_2,
          paddingValue));

  return out;
}

template <int MY_ROLE, int SOURCE_ROLE>
const std::vector<std::vector<emp::Integer>>
privatelyShareIntArraysNoPaddingFrom(
    const std::vector<std::vector<int64_t>>& in,
    int64_t numVals,
    int64_t arraySize,
    int bitLen) {
  const auto receiveStr = MY_ROLE == SOURCE_ROLE ? "sending" : "receiving";
  XLOGF(
      DBG, "Privately {} array[{}][size({})]", receiveStr, numVals, arraySize);

  int64_t flattenedLength = 0;
  std::vector<int64_t> arraysFlattened;

  arraysFlattened.reserve(numVals * arraySize);

  for (auto i = 0; i < numVals; i++) {
    if constexpr (MY_ROLE == SOURCE_ROLE) {
      auto vec = in.at(i);

      if (vec.size() != arraySize) {
        throw std::runtime_error(fmt::format(
            "Input array {} of length {} does not have required size {}",
            i,
            vec.size(),
            arraySize));
      }

      // Flatten the arrays so that it can be sent over in just one
      // batch
      arraysFlattened.insert(arraysFlattened.end(), vec.begin(), vec.end());
    }
    flattenedLength += arraySize;
  }

  // Send over the arrays
  XLOGF(DBG, "{} arrays", receiveStr);

  const std::vector<emp::Integer> arrayReceived =
      privatelyShareIntsFrom<MY_ROLE, SOURCE_ROLE>(
          arraysFlattened, flattenedLength, bitLen);

  // Un-flatten the arrays
  std::vector<std::vector<emp::Integer>> out;
  out.reserve(numVals * arraySize);
  auto it = arrayReceived.begin();
  for (auto i = 0; i < numVals; ++i) {
    std::vector<emp::Integer> array;
    array.insert(array.end(), it, it + arraySize);
    out.push_back(array);

    it += arraySize;
  }

  return out;
}

template <typename T, typename S>
void zip(
    const std::vector<T>& vec1,
    const std::vector<S>& vec2,
    std::function<void(T, S)> map_fn) {
  assert(vec1.size() == vec2.size());

  // Apply the map function
  for (int i = 0; i < vec1.size(); ++i) {
    map_fn(vec1[i], vec2[i]);
  }
}

template <typename T, typename O>
const std::vector<O> map(
    const std::vector<T>& vec,
    std::function<O(T)> map_fn) {
  // Apply the map function
  std::vector<O> out;
  out.reserve(vec.size());
  for (int i = 0; i < vec.size(); ++i) {
    out.push_back(map_fn(vec[i]));
  }
  return out;
}

template <typename T, typename S, typename O>
const std::vector<O> zip_and_map(
    const std::vector<T>& vec1,
    const std::vector<S>& vec2,
    std::function<O(T, S)> map_fn) {
  assert(vec1.size() == vec2.size());

  // Apply the map function
  std::vector<O> out;
  for (int i = 0; i < vec1.size(); ++i) {
    out.push_back(map_fn(vec1[i], vec2[i]));
  }

  return out;
}

template <typename T, typename S, typename O, typename N>
const std::pair<std::vector<O>, std::vector<N>> zip_and_map(
    const std::vector<T>& vec1,
    const std::vector<S>& vec2,
    std::function<std::pair<O, N>(T, S)> map_fn) {
  assert(vec1.size() == vec2.size());

  // Apply the map function
  std::pair<std::vector<O>, std::vector<N>> out;
  for (int i = 0; i < vec1.size(); ++i) {
    auto res = map_fn(vec1[i], vec2[i]);
    out.first.push_back(res.first);
    out.second.push_back(res.second);
  }

  return out;
}

template <typename T, typename S, typename O1, typename O2, typename O3>
const std::tuple<std::vector<O1>, std::vector<O2>, std::vector<O3>> zip_and_map(
    const std::vector<T>& vec1,
    const std::vector<S>& vec2,
    std::function<std::tuple<O1, O2, O3>(T, S)> map_fn) {
  assert(vec1.size() == vec2.size());

  // Apply the map function
  std::tuple<std::vector<O1>, std::vector<O2>, std::vector<O3>> out;
  for (int i = 0; i < vec1.size(); ++i) {
    auto res = map_fn(vec1[i], vec2[i]);
    std::get<0>(out).push_back(std::get<0>(res));
    std::get<1>(out).push_back(std::get<1>(res));
    std::get<2>(out).push_back(std::get<2>(res));
  }

  return out;
}

template <typename T, typename S, typename R, typename O>
const std::vector<O> zip_and_map(
    const std::vector<T>& vec1,
    const std::vector<S>& vec2,
    const std::vector<R>& vec3,
    std::function<O(T, S, R)> map_fn) {
  assert(vec1.size() == vec2.size());
  assert(vec1.size() == vec3.size());

  // Apply the map function
  std::vector<O> out;
  for (int i = 0; i < vec1.size(); ++i) {
    out.push_back(map_fn(vec1[i], vec2[i], vec3[i]));
  }

  return out;
}

// Partial template specialization for emp::Integer
template <>
inline const std::vector<emp::Integer> multiplyBitmask(
    const std::vector<emp::Integer>& vec,
    const std::vector<emp::Bit>& bitmask) {
  assert(vec.size() == bitmask.size());

  std::vector<emp::Integer> out;
  out.reserve(vec.size());

  const emp::Integer zero{INT_SIZE, 0, emp::PUBLIC};

  for (auto i = 0; i < vec.size(); ++i) {
    // emp::If(condition, true_case, false_case)
    out.push_back(emp::If(bitmask.at(i), vec.at(i), zero));
  }
  return out;
}

// Partial template specialization for emp::Bit
template <>
inline const std::vector<emp::Bit> multiplyBitmask(
    const std::vector<emp::Bit>& vec,
    const std::vector<emp::Bit>& bitmask) {
  assert(vec.size() == bitmask.size());

  std::vector<emp::Bit> out;
  out.reserve(vec.size());
  for (auto i = 0; i < vec.size(); ++i) {
    out.push_back(vec.at(i) & bitmask.at(i));
  }
  return out;
}

// Partial template specialization for std::vector<emp::Integer>
template <>
inline const std::vector<std::vector<emp::Integer>> multiplyBitmask(
    const std::vector<std::vector<emp::Integer>>& vec,
    const std::vector<emp::Bit>& bitmask) {
  assert(vec.size() == bitmask.size());

  std::vector<std::vector<emp::Integer>> out;
  out.reserve(vec.size());
  for (auto i = 0; i < vec.size(); ++i) {
    out.emplace_back();
    const emp::Integer zero{INT_SIZE, 0, emp::PUBLIC};
    for (auto j = 0; j < vec.at(i).size(); ++j) {
      out.back().push_back(zero.select(bitmask.at(i), vec.at(i).at(j)));
    }
  }
  return out;
}

// Partial template specialization for std::vector<emp::Bit>
template <>
inline const std::vector<std::vector<emp::Bit>> multiplyBitmask(
    const std::vector<std::vector<emp::Bit>>& vec,
    const std::vector<emp::Bit>& bitmask) {
  assert(vec.size() == bitmask.size());

  std::vector<std::vector<emp::Bit>> out;
  out.reserve(vec.size());
  for (auto i = 0; i < vec.size(); ++i) {
    out.emplace_back();
    for (auto j = 0; j < vec.at(i).size(); ++j) {
      out.back().push_back(vec.at(i).at(j) & bitmask.at(i));
    }
  }
  return out;
}

} // namespace private_measurement::secret_sharing

#endif // SECRET_SHARING_HPP
