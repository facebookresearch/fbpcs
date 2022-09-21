/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include "fbpcs/emp_games/lift/pcf2_calculator/input_processing/LiftGameProcessedData.h"

namespace private_lift {

template <int schedulerId>
class IInputProcessor {
 public:
  virtual ~IInputProcessor() = default;

  virtual const LiftGameProcessedData<schedulerId>& getLiftGameProcessedData()
      const = 0;
};

template <int schedulerId>
void writeToCSV(
    const IInputProcessor<schedulerId>& inputProcessor,
    const std::string& globalParamsPath,
    const std::string& secretSharesPath) {
  inputProcessor.getLiftGameProcessedData().writeToCSV(
      globalParamsPath, secretSharesPath);
}

} // namespace private_lift
