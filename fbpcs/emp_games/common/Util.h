/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <cstdlib>
#include <memory>
#include <sstream>

#include "folly/dynamic.h"
#include "folly/logging/xlog.h"

#include "fbpcf/engine/communication/SocketPartyCommunicationAgent.h"
#include "fbpcf/frontend/mpcGame.h"
#include "fbpcs/emp_games/common/Csv.h"
#include "fbpcs/emp_games/common/SchedulerStatistics.h"

namespace common {

// utility method used for parsing string information to vector of type T.
template <typename T>
static const std::vector<T> getInnerArray(const std::string& str) {
  // Strip the brackets [] before splitting into individual timestamp values
  auto innerString = str;
  innerString.erase(
      std::remove(innerString.begin(), innerString.end(), '['),
      innerString.end());
  innerString.erase(
      std::remove(innerString.begin(), innerString.end(), ']'),
      innerString.end());
  auto innerVals = private_measurement::csv::splitByComma(innerString, false);

  std::vector<T> out;

  for (const auto& innerVal : innerVals) {
    if (!innerVal.empty()) {
      T parsed = 0;
      std::istringstream iss{innerVal};
      if (std::is_unsigned<T>::value & (iss.peek() == '-')) {
        // convert negative inputs to zero
        iss.ignore();
        T parsedNegative = 0;
        iss >> parsedNegative;
        XLOGF(ERR, "Error: input is negative {}", parsedNegative);
      } else {
        iss >> parsed;
      }
      out.push_back(parsed);
    }
  }

  return out;
}

/**
 * Helper method to share array, with input type T and output type O, where O
 * can be constructed from T.
 */
template <typename T, typename O>
std::vector<O> privatelyShareArray(
    const std::vector<T>& inputArray,
    std::function<O(const T&)> constructor) {
  std::vector<O> outputArray;
  for (size_t i = 0; i < inputArray.size(); ++i) {
    outputArray.push_back(constructor(inputArray.at(i)));
  }
  return outputArray;
}

template <typename T, typename O>
std::vector<std::vector<O>> privatelyShareArrays(
    const std::vector<std::vector<T>>& inputArrays) {
  std::vector<std::vector<O>> outputArrays;

  for (size_t i = 0; i < inputArrays.size(); ++i) {
    auto inputArray = inputArrays.at(i);
    std::vector<O> outputArray;
    for (size_t j = 0; j < inputArray.size(); ++j) {
      outputArray.push_back(O{inputArray.at(j)});
    }
    outputArrays.push_back(std::move(outputArray));
  }

  return outputArrays;
}

/**
 * Share integer, with width number of bits, from sender to receiver. The
 * integer is revealed in plaintext to the receiver.
 */
template <int schedulerId, size_t width, int sender, int receiver>
uint64_t shareIntFrom(const int myRole, uint64_t input) {
  // Sender shares input
  typename fbpcf::frontend::MpcGame<
      schedulerId>::template SecUnsignedInt<width, false>
      secInput{input, sender};
  // Reveal to receiver
  uint64_t output = secInput.openToParty(receiver).getValue();
  return (myRole == sender) ? input : output;
}

/**
 * Share array of integers, with width number of bits, from sender to
 * receiver.
 */
template <int schedulerId, size_t width, int sender, int receiver>
std::vector<uint64_t> privatelyShareIntArrayFrom(
    const int myRole,
    std::vector<uint64_t>& inputArray) {
  // Share array size
  auto arraySize = shareIntFrom<schedulerId, width, sender, receiver>(
      myRole, inputArray.size());
  if (myRole == receiver) {
    inputArray.resize(arraySize);
  }
  // Reveal to receiver
  std::vector<uint64_t> outputArray;
  for (auto inputVal : inputArray) {
    outputArray.push_back(
        shareIntFrom<schedulerId, width, sender, receiver>(myRole, inputVal));
  }
  return (myRole == sender) ? inputArray : outputArray;
}

template <typename T>
std::vector<T>
padArray(const std::vector<T>& inputArray, size_t size, T paddingValue) {
  std::vector<T> paddedInput;
  for (size_t i = 0; i < size; ++i) {
    if (i < inputArray.size()) {
      paddedInput.push_back(inputArray.at(i));
    } else {
      paddedInput.push_back(paddingValue);
    }
  }
  return paddedInput;
}

template <typename T>
std::vector<std::vector<T>> padNestedArrays(
    const std::vector<std::vector<T>>& inputArrays,
    size_t numRows,
    size_t numCols,
    T paddingValue) {
  std::vector<std::vector<T>> paddedArrays;
  paddedArrays.reserve(numRows);
  for (size_t i = 0; i < inputArrays.size(); ++i) {
    std::vector<T> paddedArray(numCols);
    for (int j = 0; j < inputArrays[i].size(); j++) {
      paddedArray[j] = inputArrays[i][j];
    }

    for (int j = inputArrays[i].size(); j < numCols; j++) {
      paddedArray[j] = paddingValue;
    }
    paddedArrays.push_back(paddedArray);
  }
  for (size_t i = inputArrays.size(); i < numRows; ++i) {
    paddedArrays.push_back(std::vector<T>(numCols, paddingValue));
  }
  return paddedArrays;
}

/**
 * Privately share array of type T from sender, with secret batch output type O
 * and input size. If the input has a different size, resize it accordingly and
 * fill up additional entries with paddingValue.
 */
template <int sender, typename T, typename O>
O privatelyShareArrayWithPaddingFrom(
    const std::vector<T>& inputArray,
    size_t size,
    T paddingValue) {
  auto paddedInput = padArray(inputArray, size, paddingValue);
  return O{paddedInput, sender};
}

/**
 * Convert input arrays of dimension numRows by numCols to its transpose, of
 * dimension numCols by numRows. If the input has a different size, resize the
 * output accordingly and fill up additional entries with paddingValue.
 **/
template <typename T>
std::vector<std::vector<T>> transposeArraysWithPadding(
    const std::vector<std::vector<T>>& inputArrays,
    size_t numRows,
    size_t numCols,
    T paddingValue) {
  std::vector<std::vector<T>> outputArrays;
  for (size_t i = 0; i < numCols; ++i) {
    std::vector<T> outputArray;
    for (size_t j = 0; j < numRows; ++j) {
      if (inputArrays.size() > j && inputArrays.at(j).size() > i) {
        outputArray.push_back(inputArrays.at(j).at(i));
      } else {
        outputArray.push_back(paddingValue);
      }
    }
    outputArrays.push_back(std::move(outputArray));
  }
  return outputArrays;
}

template <typename T>
std::vector<std::vector<T>> transpose(const std::vector<std::vector<T>>& data) {
  std::vector<std::vector<T>> result;
  if (data.size() == 0) {
    return result;
  }

  result.reserve(data[0].size());
  for (size_t column = 0; column < data[0].size(); column++) {
    std::vector<T> innerArray(data.size());
    result.push_back(std::vector<T>(data.size()));
    for (size_t row = 0; row < data.size(); row++) {
      result[column][row] = data[row][column];
    }
  }
  return result;
}

/**
 * Privately share tranposed array of arrays of type T from sender, with batch
 * output type O. The input arrays have dimension numRows by numCols, and are
 * first tranposed before sharing. If the input has a different size, resize the
 * transposed arrays accordingly and fill up additional entries with
 * paddingValue.
 */
template <int sender, typename T, typename O>
std::vector<O> privatelyShareTransposedArraysWithPaddingFrom(
    const std::vector<std::vector<T>>& inputArrays,
    size_t numRows,
    size_t numCols,
    T paddingValue) {
  auto transposedInputArrays = transposeArraysWithPadding<T>(
      inputArrays, numRows, numCols, paddingValue);
  std::vector<O> output;
  for (auto& transposedInputArray : transposedInputArrays) {
    output.push_back(O{transposedInputArray, sender});
  }
  return output;
}

template <typename T, typename O>
T createPublicBatchConstant(O ele, size_t size) {
  std::vector<O> copies(size, ele);
  return T(copies);
}

template <typename T, typename O>
T createSecretBatchConstant(O ele, size_t size, int partyId) {
  std::vector<O> copies(size, ele);
  return T(copies, partyId);
}

/**
 * Convert a vector to a string, used for debug logging.
 */
template <typename T>
std::string vecToString(const std::vector<T>& in) {
  std::stringstream out;
  out << "[";
  for (auto j = 0U; j < in.size(); j++) {
    out << in[j];
    if (j + 1 < in.size()) {
      out << ", ";
    }
  }
  out << "]";

  return out.str();
}

inline folly::dynamic getCostExtraInfo(
    std::string party,
    std::string inputBasePath,
    std::string outputBasePath,
    int numFiles,
    int fileStartIndex,
    int concurrency,
    bool useXorEncryption,
    common::SchedulerStatistics schedulerStatistics) {
  return folly::dynamic::object(
      "publisher_input_basepath", (party == "Publisher") ? inputBasePath : "")(
      "partner_input_basepath", (party == "Partner") ? inputBasePath : "")(
      "publisher_output_basepath",
      (party == "Publisher") ? outputBasePath : "")(
      "partner_output_basepath", (party == "Partner") ? outputBasePath : "")(
      "num_files", numFiles)("file_start_index", fileStartIndex)(
      "concurrency", concurrency)("use_xor_encryption", useXorEncryption)(
      "non_free_gates", schedulerStatistics.nonFreeGates)(
      "free_gates", schedulerStatistics.freeGates)(
      "scheduler_transmitted_network", schedulerStatistics.sentNetwork)(
      "scheduler_received_network", schedulerStatistics.receivedNetwork)(
      "mpc_traffic_details", schedulerStatistics.details);
}

} // namespace common
