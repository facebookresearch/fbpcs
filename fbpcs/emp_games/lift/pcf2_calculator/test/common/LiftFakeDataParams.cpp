/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "fbpcs/emp_games/lift/pcf2_calculator/test/common/LiftFakeDataParams.h"

namespace private_lift {

LiftFakeDataParams& LiftFakeDataParams::setNumRows(size_t numRows) {
  numRows_ = numRows;
  return *this;
}

LiftFakeDataParams& LiftFakeDataParams::setOpportunityRate(
    double opportunityRate) {
  opportunityRate_ = opportunityRate;
  return *this;
}

LiftFakeDataParams& LiftFakeDataParams::setTestRate(double testRate) {
  testRate_ = testRate;
  return *this;
}

LiftFakeDataParams& LiftFakeDataParams::setPurchaseRate(double purchaseRate) {
  purchaseRate_ = purchaseRate;
  return *this;
}

LiftFakeDataParams& LiftFakeDataParams::setIncrementalityRate(
    double incrementalityRate) {
  incrementalityRate_ = incrementalityRate;
  return *this;
}

LiftFakeDataParams& LiftFakeDataParams::setEpoch(int32_t epoch) {
  epoch_ = epoch;
  return *this;
}

LiftFakeDataParams& LiftFakeDataParams::setNumConversions(
    int32_t numConversions) {
  numConversions_ = numConversions;
  return *this;
}

LiftFakeDataParams& LiftFakeDataParams::setOmitValuesColumn(
    bool omitValuesColumn) {
  omitValuesColumn_ = omitValuesColumn;
  return *this;
}

LiftFakeDataParams& LiftFakeDataParams::setNumBreakdowns(
    int32_t numBreakdowns) {
  numBreakdowns_ = numBreakdowns;
  return *this;
}

LiftFakeDataParams& LiftFakeDataParams::setNumCohorts(int32_t numCohorts) {
  numCohorts_ = numCohorts;
  return *this;
}

} // namespace private_lift
