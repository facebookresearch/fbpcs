/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <cmath>

namespace private_lift {

class LiftFakeDataParams {
 public:
  size_t numRows_ = 10;
  double opportunityRate_ = 0.8;
  double testRate_ = 0.5;
  double purchaseRate_ = 0.3;
  double incrementalityRate_ = 0.1;
  int32_t epoch_ = 0;
  int32_t numConversions_ = 4;
  bool omitValuesColumn_ = false;
  int32_t numBreakdowns_ = 0;
  int32_t numCohorts_ = 0;

  LiftFakeDataParams& setNumRows(size_t numRows);
  LiftFakeDataParams& setOpportunityRate(double opportunityRate);
  LiftFakeDataParams& setTestRate(double testRate);
  LiftFakeDataParams& setPurchaseRate(double purchaseRate);
  LiftFakeDataParams& setIncrementalityRate(double incrementalityRate);
  LiftFakeDataParams& setEpoch(int32_t epoch);
  LiftFakeDataParams& setNumConversions(int32_t numConversions);
  LiftFakeDataParams& setOmitValuesColumn(bool omitValuesColumn);
  LiftFakeDataParams& setNumBreakdowns(int32_t numBreakdowns);
  LiftFakeDataParams& setNumCohorts(int32_t numCohorts);
};

} // namespace private_lift
