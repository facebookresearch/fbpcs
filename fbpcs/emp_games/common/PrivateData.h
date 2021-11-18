/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <limits>
#include <optional>
#include <sstream>

#include <emp-sh2pc/emp-sh2pc.h>
#include "folly/logging/xlog.h"

namespace private_measurement {

constexpr int64_t INT_SIZE = 64;
const int64_t MIN_INT = std::numeric_limits<int64_t>::min();
const int64_t MAX_INT = std::numeric_limits<int64_t>::max();

/*
 * Helper function to get the opposite role from the input role
 */
constexpr int otherRole(int role) {
  return role == emp::ALICE ? emp::BOB : emp::ALICE;
}

inline const std::string roleString(int role) {
  return (role == emp::ALICE) ? "ALICE"
                              : ((role == emp::BOB) ? "BOB" : "UNKNOWN");
}

/**
 * Converts a private vector to a string. T must support equality checking via
 * ==. If a null value is passed, elements that equal that value are printed
 * out as ✗.
 */
template <typename T>
std::string vecToString(
    const std::vector<T>& in,
    const std::optional<T> nullValue = std::nullopt) {
  std::stringstream out;

  out << "[";
  for (std::size_t j = 0; j < in.size(); j++) {
    const auto& val = in[j];
    if (nullValue.has_value() && val == nullValue.value()) {
      out << "✗";
    } else {
      out << in[j];
    }

    if (j + 1 < in.size()) {
      out << ", ";
    }
  }

  out << "]";

  return out.str();
}

/**
 * Converts a private vector to a string. T must support output to ostream
 * and comparison via ==. if a null value is passed, elements that equal that
 * value are printed out as ✗.
 */
template <int MY_ROLE, int SOURCE_ROLE, typename T>
std::string privateVecToString(
    const std::vector<T>& in,
    int64_t numVals,
    const std::optional<T> nullValue = std::nullopt) {
  if (MY_ROLE == SOURCE_ROLE) {
    return vecToString(in, nullValue);
  } else {
    std::stringstream out;
    out << "[" << numVals << " HIDDEN]";
    return out.str();
  }
}

/**
 * Converts a private vector to a string. T must have support revealing via
 * T::reveal<string>(SOURCE_ROLE).
 */
template <int MY_ROLE, int SOURCE_ROLE, typename T>
std::string privateVecToString(const std::vector<T>& in) {
  int64_t numVals = in.size();
  std::vector<std::string> revealedVals;

  // Reveal each of the elements
  for (auto j = 0; j < numVals; j++) {
    auto revealedVal = in[j].template reveal<std::string>(SOURCE_ROLE);
    revealedVals.push_back(revealedVal);
  }

  return privateVecToString<MY_ROLE, SOURCE_ROLE, std::string>(
      revealedVals, numVals);
}

/*
 * This class is an abstraction over private input when data is shared
 * bidirectionally between two parties. Data can be accessed through the
 * publisher/partner value datatypes.
 */
template <typename T, int ROLE>
class PrivateData {
 public:
  PrivateData(T myValue, T theirValue)
      : myValue_(myValue), theirValue_(theirValue) {}

  T getPublisherValue() {
    return ROLE == emp::ALICE ? myValue_ : theirValue_;
  }

  T getPartnerValue() {
    return ROLE == emp::ALICE ? theirValue_ : myValue_;
  }

 private:
  T myValue_;
  T theirValue_;
};

/*
 * Specialization of PrivateData for dealing with emp::Integer
 */
template <int ROLE>
class PrivateInt : public PrivateData<emp::Integer, ROLE> {
 public:
  PrivateInt(emp::Integer myValue, emp::Integer theirValue)
      : PrivateData<emp::Integer, ROLE>{myValue, theirValue} {}

  // Specialized constructor since creating a PrivateInt from int64_t is such
  // a common operation
  PrivateInt(int64_t myValue, int64_t theirValue)
      : PrivateData<emp::Integer, ROLE>{
            emp::Integer(INT_SIZE, myValue, ROLE),
            emp::Integer(INT_SIZE, theirValue, otherRole(ROLE))} {}

  emp::Integer publisherInt() {
    return PrivateData<emp::Integer, ROLE>::getPublisherValue();
  }

  emp::Integer partnerInt() {
    return PrivateData<emp::Integer, ROLE>::getPartnerValue();
  }
};

/*
 * Specialization of PrivateData for dealing with emp::Bit
 */
template <int ROLE>
class PrivateBit : public PrivateData<emp::Bit, ROLE> {
 public:
  PrivateBit(emp::Bit myValue, emp::Bit theirValue)
      : PrivateData<emp::Bit, ROLE>{myValue, theirValue} {}

  emp::Bit publisherBit() {
    return PrivateData<emp::Bit, ROLE>::getPublisherValue();
  }

  emp::Bit partnerBit() {
    return PrivateData<emp::Bit, ROLE>::getPartnerValue();
  }
};

} // namespace private_measurement
