/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include "folly/logging/xlog.h"

#include "fbpcs/emp_games/lift/pcf2_calculator/InputProcessor.h"

namespace private_lift {

template <int schedulerId>
class Attributor {
 public:
  Attributor(int myRole, InputProcessor<schedulerId> inputProcessor)
      : myRole_{myRole},
        inputProcessor_{inputProcessor},
        numRows_{inputProcessor.getNumRows()} {
    calculateEvents();
    calculateNumConvSquaredAndValueSquaredAndConverters();
    calculateMatch();
    calculateReachedConversions();
    calculateValues();
  }

  const std::vector<SecBit<schedulerId>> getEvents() const {
    return events_;
  }

  const SecBit<schedulerId> getConverters() const {
    return converters_;
  }

  const SecNumConvSquared<schedulerId> getNumConvSquared() const {
    return numConvSquared_;
  }

  const SecBit<schedulerId> getMatch() const {
    return match_;
  }

  const std::vector<SecBit<schedulerId>> getReachedConversions() const {
    return reachedConversions_;
  }

  const std::vector<SecValue<schedulerId>> getValues() const {
    return values_;
  }

  const std::vector<SecValue<schedulerId>> getReachedValues() const {
    return reachedValues_;
  }

  const SecValueSquared<schedulerId> getValueSquared() const {
    return valueSquared_;
  }

 private:
  // Test/Control events: validPurchase (oppTs < purchaseTs + 10)
  void calculateEvents();

  // Test/Control numConvSquared: number of valid events squared
  // Test/Control converters: any valid event
  // Test/Control value squared: sum(valid event ? purchaseValue : 0)^2
  void calculateNumConvSquaredAndValueSquaredAndConverters();

  // Test/control match: valid opportunity timestamp & any valid purchase
  // timestamp
  void calculateMatch();

  // Test reached conversions: valid event & reach (number of impressions > 0)
  void calculateReachedConversions();

  // Test/control value: valid event ? purchaseValue : 0
  // Test reached value: isReached ? purchaseValue : 0
  void calculateValues();

  int32_t myRole_;
  InputProcessor<schedulerId> inputProcessor_;
  int64_t numRows_;

  std::vector<SecBit<schedulerId>> events_;
  SecBit<schedulerId> converters_;
  SecNumConvSquared<schedulerId> numConvSquared_;
  SecBit<schedulerId> match_;
  std::vector<SecBit<schedulerId>> reachedConversions_;
  std::vector<SecValue<schedulerId>> values_;
  std::vector<SecValue<schedulerId>> reachedValues_;
  SecValueSquared<schedulerId> valueSquared_;
};

} // namespace private_lift

#include "fbpcs/emp_games/lift/pcf2_calculator/Attributor_impl.h"
