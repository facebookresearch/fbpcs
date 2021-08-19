/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "Timestamp.h"
#include "folly/logging/xlog.h"

namespace measurement::private_attribution {

int Timestamp::length() const {
  return ts_.size();
}

emp::Bit Timestamp::geq(const Timestamp& rhs) const {
  checkComparable(rhs);
  return ts_.geq(rhs.ts_);
}

emp::Bit Timestamp::equal(const Timestamp& rhs) const {
  checkComparable(rhs);
  return ts_.equal(rhs.ts_);
}

emp::Bit Timestamp::operator<(const int64_t rhs) const {
  return !geq(Timestamp{rhs, minValue_, maxValue_, precision_});
}

emp::Bit operator>(int64_t lhs, const Timestamp& rhs) {
  // Defer to the well-defined Timestamp::operator< function call
  return rhs < lhs;
}

Timestamp Timestamp::select(const emp::Bit& sel, const Timestamp& rhs) const {
  checkComparable(rhs);
  return Timestamp{minValue_, maxValue_, precision_, ts_.select(sel, rhs.ts_)};
}

Timestamp Timestamp::operator-(const Timestamp& rhs) const {
  checkComparable(rhs);
  return Timestamp{minValue_, maxValue_, precision_, ts_ - rhs.ts_};
}

void Timestamp::checkComparable(const Timestamp& rhs) const {
  if (minValue_ != rhs.minValue_ || maxValue_ != rhs.maxValue_ ||
      precision_ != rhs.precision_ || length() != rhs.length()) {
    XLOG(FATAL) << "Timestamps not comparable";
  }
}

} // namespace measurement::private_attribution
