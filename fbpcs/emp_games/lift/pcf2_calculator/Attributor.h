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
    calculateNumConvSquaredAndConverters();
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

 private:
  // Test/Control events: validPurchase (oppTs < purchaseTs + 10)
  void calculateEvents();

  // Test/Control numConvSquared: number of valid events squared
  // Test/Control converters: any valid event
  void calculateNumConvSquaredAndConverters();

  int32_t myRole_;
  InputProcessor<schedulerId> inputProcessor_;
  int64_t numRows_;

  std::vector<SecBit<schedulerId>> events_;
  SecBit<schedulerId> converters_;
  SecNumConvSquared<schedulerId> numConvSquared_;
};

} // namespace private_lift

#include "fbpcs/emp_games/lift/pcf2_calculator/Attributor_impl.h"
