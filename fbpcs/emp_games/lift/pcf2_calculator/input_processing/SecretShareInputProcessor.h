/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "fbpcs/emp_games/lift/pcf2_calculator/input_processing/IInputProcessor.h"

#pragma once

namespace private_lift {

template <int schedulerId>
class SecretShareInputProcessor : public IInputProcessor<schedulerId> {
 public:
  SecretShareInputProcessor(
      const std::string& globalParamsPath,
      const std::string& secretSharePath)
      : liftGameProcessedData_{LiftGameProcessedData<schedulerId>::readFromCSV(
            globalParamsPath,
            secretSharePath)} {}

  SecretShareInputProcessor() {}

  const LiftGameProcessedData<schedulerId>& getLiftGameProcessedData() const {
    return liftGameProcessedData_;
  }

 private:
  LiftGameProcessedData<schedulerId> liftGameProcessedData_;
};

} // namespace private_lift
