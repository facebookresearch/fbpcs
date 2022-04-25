/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include "folly/logging/xlog.h"

#include "fbpcs/emp_games/common/Constants.h"
#include "fbpcs/emp_games/common/Util.h"
#include "fbpcs/emp_games/lift/pcf2_calculator/Constants.h"
#include "fbpcs/emp_games/lift/pcf2_calculator/InputData.h"

namespace private_lift {
/**
 * This class handles privately sharing all the input data in MPC.
 */
template <int schedulerId>
class InputProcessor {
 public:
  InputProcessor(int myRole, InputData inputData, int32_t numConversionsPerUser)
      : myRole_{myRole},
        inputData_{inputData},
        numRows_{inputData.getNumRows()},
        numConversionsPerUser_{numConversionsPerUser} {
    validateNumRowsStep();
    privatelyShareTestReachStep();
  }

  InputProcessor() {}

  int64_t getNumRows() const {
    return numRows_;
  }

  const SecBit<schedulerId> getTestReach() const {
    return testReach_;
  }

 private:
  // Make sure input files have the same size
  void validateNumRowsStep();

  // Privately share test reach (nonzero impressions)
  void privatelyShareTestReachStep();

  int32_t myRole_;
  InputData inputData_;
  int64_t numRows_;
  int32_t numConversionsPerUser_;

  SecBit<schedulerId> testReach_;
};

} // namespace private_lift

#include "fbpcs/emp_games/lift/pcf2_calculator/InputProcessor_impl.h"
