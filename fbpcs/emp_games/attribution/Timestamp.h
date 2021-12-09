/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <cstdint>
#include <limits>
#include "folly/Traits.h"

#include <emp-sh2pc/emp-sh2pc.h>

namespace measurement::private_attribution {

enum class Precision { SECONDS = 1, MINUTES = 60, HOURS = 3600 };

constexpr int64_t constexpr_ceil(double num) {
  return (static_cast<double>(static_cast<int64_t>(num)) == num)
      ? static_cast<int64_t>(num)
      : static_cast<int64_t>(num) + ((num > 0) ? 1 : 0);
}

// Computes the number of bits needed to store the values:
// minValue, minValue + p, minValue + 2p, ..., maxValue
constexpr int32_t bitsNeeded(int64_t minValue, int64_t maxValue, Precision p) {
  assert(minValue <= maxValue);
  return constexpr_ceil(
      // Cast to int128_t to prevent overflow
      log2(static_cast<folly::int128_t>(maxValue) - minValue + 1) -
      log2(static_cast<int32_t>(p)));
}

constexpr int64_t floorDiv(int64_t numerator, int64_t denominator) {
  if (numerator >= 0 || numerator % denominator == 0) {
    return numerator / denominator;
  }
  return numerator / denominator - 1;
}

constexpr int64_t ceilDiv(int64_t numerator, int64_t denominator) {
  if (numerator <= 0 || numerator % denominator == 0) {
    return numerator / denominator;
  }
  return numerator / denominator + 1;
}

// This function has the property that minValue <= midpoint <= maxValue
constexpr int64_t midpoint(int64_t minValue, int64_t maxValue) {
  return floorDiv(minValue, 2) + ceilDiv(maxValue, 2);
}

constexpr int64_t
scale(int64_t minValue, int64_t maxValue, Precision p, int64_t ts) {
  assert(minValue <= maxValue);
  // TODO T92901160 - Should this throw an exception?
  int64_t boundedTs = std::min(std::max(ts, minValue), maxValue);
  return floorDiv(
      boundedTs - midpoint(minValue, maxValue), static_cast<int64_t>(p));
}

constexpr int64_t
unscale(int64_t minValue, int64_t maxValue, Precision p, int64_t scaledTs) {
  assert(minValue <= maxValue);
  return scaledTs * static_cast<int64_t>(p) + midpoint(minValue, maxValue);
}

// TODO T92901160: Move this to pcf library
class Timestamp : public emp::Swappable<Timestamp>,
                  public emp::Comparable<Timestamp> {
 private:
  Timestamp(
      int64_t minValue,
      int64_t maxValue,
      Precision p,
      const emp::Integer& ts)
      : minValue_{minValue}, maxValue_{maxValue}, precision_{p}, ts_{ts} {}

 public:
  explicit Timestamp(
      int64_t ts,
      int party = emp::ALICE,
      int64_t minValue = kDefaultMinValue,
      int64_t maxValue = kDefaultMaxValue,
      Precision p = kDefaultPrecision)
      : Timestamp{
            minValue,
            maxValue,
            p,
            emp::Integer{
                bitsNeeded(minValue, maxValue, p),
                scale(minValue, maxValue, p, ts),
                party,
            },
        } {}

  explicit Timestamp(
      const emp::block* b,
      int64_t minValue = kDefaultMinValue,
      int64_t maxValue = kDefaultMaxValue,
      Precision p = kDefaultPrecision)
      : Timestamp{
            minValue,
            maxValue,
            p,
            emp::Integer{bitsNeeded(minValue, maxValue, p), b}} {}

  int length() const;

  // Comparable
  emp::Bit geq(const Timestamp& rhs) const;
  emp::Bit equal(const Timestamp& rhs) const;
  emp::Bit operator<(int64_t rhs) const;
  friend emp::Bit operator>(int64_t lhs, const Timestamp& rhs);

  // Swappable
  Timestamp select(const emp::Bit& sel, const Timestamp& rhs) const;

  template <typename O>
  O reveal(int party = emp::PUBLIC) const;

  Timestamp operator-(const Timestamp& rhs) const;

 private:
  int64_t minValue_;
  int64_t maxValue_;
  Precision precision_;
  emp::Integer ts_;

  static const int64_t kDefaultMinValue = std::numeric_limits<int64_t>::min();
  static const int64_t kDefaultMaxValue = std::numeric_limits<int64_t>::max();
  static const Precision kDefaultPrecision = Precision::SECONDS;

  void checkComparable(const Timestamp& rhs) const;
};

template <typename T>
inline T Timestamp::reveal(int party) const {
  return unscale(minValue_, maxValue_, precision_, ts_.reveal<T>(party));
}

template <>
inline string Timestamp::reveal<string>(int party) const {
  return std::to_string(Timestamp::reveal<int64_t>(party));
}

} // namespace measurement::private_attribution
