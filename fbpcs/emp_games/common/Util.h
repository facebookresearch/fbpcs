/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <sstream>

#include "folly/dynamic.h"

#include "fbpcf/frontend/mpcGame.h"
#include "fbpcs/emp_games/common/Csv.h"
#include "fbpcs/emp_games/common/SchedulerStatistics.h"

namespace common {

// utility method used for parsing string information to vector of type T.
template <typename T>
static const std::vector<T> getInnerArray(std::string& str) {
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
std::vector<O> privatelyShareArray(const std::vector<T>& inputArray) {
  std::vector<O> outputArray;
  for (size_t i = 0; i < inputArray.size(); ++i) {
    outputArray.push_back(O{inputArray.at(i)});
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
  std::vector<T> paddedInput;
  for (size_t i = 0; i < size; ++i) {
    if (i < inputArray.size()) {
      paddedInput.push_back(inputArray.at(i));
    } else {
      paddedInput.push_back(paddingValue);
    }
  }
  return O{paddedInput, sender};
}

/**
 * Convert a vector to a string, used for debug logging.
 */
template <typename T>
std::string vecToString(const std::vector<T>& in) {
  std::stringstream out;
  out << "[";
  for (auto j = 0; j < in.size(); j++) {
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
        "publisher_input_basepath", (party == "Publisher") ? inputBasePath : "")
        ("partner_input_basepath", (party == "Partner") ? inputBasePath : "")
        ("publisher_output_basepath", (party == "Publisher") ? outputBasePath : "")
        ("partner_output_basepath", (party == "Partner") ? outputBasePath : "")
        ("num_files", numFiles)
        ("file_start_index", fileStartIndex)
        ("concurrency", concurrency)
        ("use_xor_encryption", useXorEncryption)
        ("non_free_gates", schedulerStatistics.nonFreeGates)
        ("free_gates", schedulerStatistics.freeGates)
        ("scheduler_transmitted_network", schedulerStatistics.sentNetwork)
        ("scheduler_received_network", schedulerStatistics.receivedNetwork);
}

} // namespace common
