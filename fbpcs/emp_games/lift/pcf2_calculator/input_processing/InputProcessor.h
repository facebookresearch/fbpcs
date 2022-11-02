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
#include "fbpcs/emp_games/lift/pcf2_calculator/input_processing/Constants.h"
#include "fbpcs/emp_games/lift/pcf2_calculator/input_processing/GlobalSharingUtils.h"
#include "fbpcs/emp_games/lift/pcf2_calculator/input_processing/IInputProcessor.h"
#include "fbpcs/emp_games/lift/pcf2_calculator/input_processing/InputData.h"

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

    input_processing::validateNumRowsStep(myRole_, liftGameProcessedData_);
    input_processing::shareNumGroupsStep(
        myRole_, inputData_, liftGameProcessedData_);
    input_processing::shareBitsForValuesStep(
        myRole_, inputData_, liftGameProcessedData_);

    privatelyShareGroupIdsStep();
    privatelySharePopulationStep();
    input_processing::computeIndexSharesAndSetTestGroupIds(
        liftGameProcessedData_,
        cohortGroupIds_,
        controlPopulation_,
        breakdownGroupIds_,
        testGroupIds_);
    input_processing::computeTestIndexShares(
        liftGameProcessedData_, controlPopulation_, testGroupIds_);
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

#include "fbpcs/emp_games/lift/pcf2_calculator/input_processing/InputProcessor_impl.h"
