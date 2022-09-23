/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <memory>

#include "fbpcf/frontend/mpcGame.h"
#include "fbpcs/emp_games/lift/pcf2_calculator/input_processing/InputData.h"
#include "fbpcs/emp_games/lift/pcf2_calculator/input_processing/InputProcessor.h"

namespace private_lift {

template <int schedulerId>
class IMetadataCompactorGame {
 public:
  virtual ~IMetadataCompactorGame() = default;

  virtual std::unique_ptr<IInputProcessor<schedulerId>> play(
      InputData inputData,
      int32_t numConversionPerUser) = 0;
};

} // namespace private_lift
