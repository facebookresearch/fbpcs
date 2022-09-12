/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <fbpcs/emp_games/lift/pcf2_calculator/Constants.h>
#include "folly/logging/xlog.h"

#include "fbpcs/emp_games/common/Constants.h"
#include "fbpcs/emp_games/common/Util.h"
#include "fbpcs/emp_games/lift/pcf2_calculator/Constants.h"
#include "fbpcs/emp_games/lift/pcf2_calculator/IInputProcessor.h"
#include "fbpcs/emp_games/lift/pcf2_calculator/InputData.h"

namespace private_lift {
/**
 * This class handles privately sharing all the input data in MPC.
 */
template <int schedulerId>
class InputProcessor : public IInputProcessor<schedulerId> {
 public:
  InputProcessor(int myRole, InputData inputData, int32_t numConversionsPerUser)
      : myRole_{myRole},
        inputData_{inputData},
        numConversionsPerUser_{numConversionsPerUser} {
    liftGameProcessedData_.numRows = inputData.getNumRows();

    validateNumRowsStep();
    shareNumGroupsStep();
    shareBitsForValuesStep();
    privatelyShareGroupIdsStep();
    privatelySharePopulationStep();
    privatelyShareIndexSharesStep();
    privatelyShareTestIndexSharesStep();
    privatelyShareTimestampsStep();
    privatelySharePurchaseValuesStep();
    privatelyShareTestReachStep();
  }

  InputProcessor() {}

  const LiftGameProcessedData<schedulerId>& getLiftGameProcessedData()
      const override {
    return liftGameProcessedData_;
  }

 private:
  // Make sure input files have the same size
  void validateNumRowsStep();

  // Share number of groups, including cohorts and publisher breakdowns.
  void shareNumGroupsStep();

  // Share number of bits needed to store the input value and its square
  void shareBitsForValuesStep();

  // Privately share cohort ids and breakdown ids.
  void privatelyShareGroupIdsStep();

  // Privately share popoulation
  void privatelySharePopulationStep();

  // Privately share index shares of group ids encoding the population, cohorts
  // and publisher breakdowns.
  void privatelyShareIndexSharesStep();

  // Privately share index shares of group ids for the test population only.
  void privatelyShareTestIndexSharesStep();

  // Privately share timestamps
  void privatelyShareTimestampsStep();

  // Privately share purchase values and purchase values squared
  void privatelySharePurchaseValuesStep();

  // Privately share test reach (nonzero impressions)
  void privatelyShareTestReachStep();

  int32_t myRole_;
  InputData inputData_;

  int32_t numConversionsPerUser_;

  SecBit<schedulerId> controlPopulation_;
  SecGroup<schedulerId> cohortGroupIds_;
  SecBit<schedulerId> breakdownGroupIds_;
  SecGroup<schedulerId> testGroupIds_;

  LiftGameProcessedData<schedulerId> liftGameProcessedData_;
};

} // namespace private_lift

#include "fbpcs/emp_games/lift/pcf2_calculator/InputProcessor_impl.h"
