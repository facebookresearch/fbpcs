/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <filesystem>

#include "InputData.h"

namespace private_lift {
/*
 * Simple struct representing the all the input arguments for a CalculatorGame
 */
struct CalculatorGameConfig {
  InputData inputData;
  bool isConversionLift;
  int32_t numConversionsPerUser;
};
} // namespace private_lift
