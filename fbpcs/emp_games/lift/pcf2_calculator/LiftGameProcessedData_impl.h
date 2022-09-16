/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <stdexcept>
#include "fbpcs/emp_games/lift/pcf2_calculator/LiftGameProcessedData.h"

namespace private_lift {

template <int schedulerId>
void LiftGameProcessedData<schedulerId>::writeToCSV(
    const std::string& globalParamsOutputPath,
    const std::string& secretSharesOutputPath) const {
  throw std::runtime_error("Unimplemented");
}

template <int schedulerId>
LiftGameProcessedData<schedulerId>
LiftGameProcessedData<schedulerId>::readFromCSV(
    const std::string& globalParamsInputPath,
    const std::string& secretSharesInputPath) {
  throw std::runtime_error("Unimplemented");
}
} // namespace private_lift
