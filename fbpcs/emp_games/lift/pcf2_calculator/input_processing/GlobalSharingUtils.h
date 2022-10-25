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
#include "fbpcs/emp_games/lift/pcf2_calculator/input_processing/IInputProcessor.h"
#include "fbpcs/emp_games/lift/pcf2_calculator/input_processing/InputData.h"

namespace private_lift::input_processing {

template <int schedulerId>
inline void validateNumRowsStep(
    int myRole,
    const LiftGameProcessedData<schedulerId>& liftGameProcessedData) {
  XLOG(INFO) << "Share number of rows";
  const size_t width = 32;
  auto publisherNumRows = common::
      shareIntFrom<schedulerId, width, common::PUBLISHER, common::PARTNER>(
          myRole, liftGameProcessedData.numRows);
  auto partnerNumRows = common::
      shareIntFrom<schedulerId, width, common::PARTNER, common::PUBLISHER>(
          myRole, liftGameProcessedData.numRows);

  if (publisherNumRows != partnerNumRows) {
    XLOG(ERR) << "The publisher has " << publisherNumRows
              << " rows in their input, while the partner has "
              << partnerNumRows << " rows.";
    exit(1);
  }
}

template <int schedulerId>
inline void shareNumGroupsStep(
    int myRole,
    const InputData& inputData,
    LiftGameProcessedData<schedulerId>& liftGameProcessedData) {
  // TODO: We shouldn't be using MPC for this, it should just be shared over
  // a normal network socket as part of the protocol setup
  XLOG(INFO) << "Set up number of partner groups";
  if (inputData.getNumGroups() >
      (1
       << (groupWidth - 1))) { // subtract one because we multiply the number of
    // groups by 2 for the test/control populations
    XLOG(ERR) << "The input has " << inputData.getNumGroups()
              << " groups but we only support " << (1 << groupWidth)
              << " groups.";
    exit(1);
  }
  liftGameProcessedData.numPartnerCohorts = common::
      shareIntFrom<schedulerId, groupWidth, common::PARTNER, common::PUBLISHER>(
          myRole, inputData.getNumGroups());
  liftGameProcessedData.numPublisherBreakdowns = common::
      shareIntFrom<schedulerId, groupWidth, common::PUBLISHER, common::PARTNER>(
          myRole, inputData.getNumGroups());
  if (liftGameProcessedData.numPublisherBreakdowns > 2) {
    XLOG(ERR)
        << "The input has " << liftGameProcessedData.numPublisherBreakdowns
        << " publisher breakdowns but we only support 2 publisher breakdowns.";
    exit(1);
  }
  // The number of groups is 2 (for test/control population) times the number of
  // partner cohorts and the number of publisher breakdowns. If there are no
  // cohorts or breakdowns, we multiply by 1 instead.
  liftGameProcessedData.numGroups = 2 *
      std::max(uint32_t(1), liftGameProcessedData.numPartnerCohorts) *
      std::max(uint32_t(1), liftGameProcessedData.numPublisherBreakdowns);
  // The test groups consist of the groups corresponding to the test population,
  // and one additional group for the control population (disregarding
  // breakdown or cohort id). These are used for computing reach metrics, which
  // are only for the test population.
  liftGameProcessedData.numTestGroups = 1 + liftGameProcessedData.numGroups / 2;
  XLOG(INFO) << "Will be computing metrics for "
             << liftGameProcessedData.numPublisherBreakdowns
             << " publisher breakdowns and "
             << liftGameProcessedData.numPartnerCohorts << " partner cohorts";
}

template <int schedulerId>
inline void shareBitsForValuesStep(
    int myRole,
    const InputData& inputData,
    LiftGameProcessedData<schedulerId>& liftGameProcessedData) {
  XLOG(INFO) << "Set up number of bits needed for purchase value sharing";

  auto valueBits = static_cast<uint64_t>(inputData.getNumBitsForValue());
  auto valueSquaredBits =
      static_cast<uint64_t>(inputData.getNumBitsForValueSquared());

  liftGameProcessedData.valueBits = common::shareIntFrom<
      schedulerId,
      numBitsForValuesWidth,
      common::PARTNER,
      common::PUBLISHER>(myRole, valueBits);
  liftGameProcessedData.valueSquaredBits = common::shareIntFrom<
      schedulerId,
      numBitsForValuesWidth,
      common::PARTNER,
      common::PUBLISHER>(myRole, valueSquaredBits);
  XLOG(INFO) << "Num bits for values: " << liftGameProcessedData.valueBits;
  XLOG(INFO) << "Num bits for values squared: "
             << liftGameProcessedData.valueSquaredBits;
}

} // namespace private_lift::input_processing
