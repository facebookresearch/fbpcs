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
  XLOG(INFO) << "Set up number of breakdowns and cohorts";
  if (inputData.getNumPartnerCohorts() >
      (1LL
       << (groupWidth - 1))) { // subtract one because we multiply the number of
    // groups by 2 for the test/control populations
    XLOG(ERR) << "The input has " << inputData.getNumPartnerCohorts()
              << " cohorts but we only support " << (1L << groupWidth)
              << " cohorts.";
    exit(1);
  }
  if (inputData.getNumPublisherBreakdowns() >
      (1LL
       << (groupWidth - 1))) { // subtract one because we multiply the number of
    // groups by 2 for the test/control populations
    XLOG(ERR) << "The input has " << inputData.getNumPublisherBreakdowns()
              << " breakdowns but we only support " << (1L << groupWidth)
              << " breakdowns.";
    exit(1);
  }
  liftGameProcessedData.numPartnerCohorts = common::
      shareIntFrom<schedulerId, groupWidth, common::PARTNER, common::PUBLISHER>(
          myRole, inputData.getNumPartnerCohorts());
  liftGameProcessedData.numPublisherBreakdowns = common::
      shareIntFrom<schedulerId, groupWidth, common::PUBLISHER, common::PARTNER>(
          myRole, inputData.getNumPublisherBreakdowns());
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
  XLOG(INFO) << "Num bits for values: "
             << (int32_t)liftGameProcessedData.valueBits;
  XLOG(INFO) << "Num bits for values squared: "
             << (int32_t)liftGameProcessedData.valueSquaredBits;
}

template <int schedulerId>
inline void computeIndexSharesAndSetTestGroupIds(
    LiftGameProcessedData<schedulerId>& liftGameProcessedData,
    const SecGroup<schedulerId>& cohortGroupIds,
    const SecBit<schedulerId>& controlPopulation,
    const SecBit<schedulerId>& breakdownGroupIds,
    SecGroup<schedulerId>& testGroupIds) {
  // We compute the metrics for test/control populations, 0/1 publisher
  // breakdowns, and partner cohorts. In order to compute the ORAM aggregation
  // for these 3 different types of groups, we have to differentiate them from
  // each other when assigning the group ids. There are up to
  // 4 * numPartnerCohorts_ group ids in total, and we assign the first
  // 2 * numPartnerCohorts_ group ids to the test population, and the second
  // half to the control population. Within the test population, we assign the
  // group ids 0 to numPartnerCohorts_ - 1 to breakdown id 0, and the group ids
  // from numPartnerCohorts_ to 2 * numPartnerCohorts_ - 1 to breakdown id 1. We
  // similarly assign the group ids for the control population.
  bool usingCohorts = liftGameProcessedData.numPartnerCohorts > 0;
  bool usingPublisherBreakdowns =
      liftGameProcessedData.numPublisherBreakdowns > 0;

  SecGroup<schedulerId> secGroupIds;

  if (usingCohorts) {
    auto pubNumPartnerCohorts =
        common::createPublicBatchConstant<PubGroup<schedulerId>>(
            liftGameProcessedData.numPartnerCohorts,
            liftGameProcessedData.numRows);

    if (usingPublisherBreakdowns) {
      // We now set the group ids depending on whether each row is a test or
      // control, and whether the breakdown id is 0 or 1.
      auto group0 = common::createPublicBatchConstant<PubGroup<schedulerId>>(
          0UL, liftGameProcessedData.numRows);

      auto breakdownMux = group0.mux(breakdownGroupIds, pubNumPartnerCohorts);
      testGroupIds = cohortGroupIds + breakdownMux;

      auto secControlGroupIds = pubNumPartnerCohorts + pubNumPartnerCohorts +
          cohortGroupIds + breakdownMux;
      secGroupIds = testGroupIds.mux(controlPopulation, secControlGroupIds);
    } else {
      testGroupIds = cohortGroupIds;
      secGroupIds = cohortGroupIds.mux(
          controlPopulation, cohortGroupIds + pubNumPartnerCohorts);
    }
  } else {
    if (usingPublisherBreakdowns) {
      // We set the publisher breakdown groups to 0, 1, 2, 3 if no cohorts
      auto group0 = common::createPublicBatchConstant<PubGroup<schedulerId>>(
          0UL, liftGameProcessedData.numRows);
      auto group1 = common::createPublicBatchConstant<PubGroup<schedulerId>>(
          1UL, liftGameProcessedData.numRows);
      auto group2 = common::createPublicBatchConstant<PubGroup<schedulerId>>(
          2UL, liftGameProcessedData.numRows);
      auto group3 = common::createPublicBatchConstant<PubGroup<schedulerId>>(
          3UL, liftGameProcessedData.numRows);

      // We now set the group ids depending on whether each row is a test or
      // control, and whether the breakdown id is 0 or 1.
      testGroupIds = group0.mux(breakdownGroupIds, group1);
      auto secControlGroupIds = group2.mux(breakdownGroupIds, group3);

      secGroupIds = testGroupIds.mux(controlPopulation, secControlGroupIds);

    } else {
      testGroupIds = cohortGroupIds; // 0

      auto group0 = common::createPublicBatchConstant<PubGroup<schedulerId>>(
          0UL, liftGameProcessedData.numRows);
      auto group1 = common::createPublicBatchConstant<PubGroup<schedulerId>>(
          1UL, liftGameProcessedData.numRows);

      secGroupIds = group0.mux(controlPopulation, group1);
    }
  }

  // Generate index shares from group ids
  liftGameProcessedData.indexShares =
      secGroupIds.extractIntShare().getBooleanShares();
  // Resize to width needed for the number of groups
  size_t groupWidth = std::ceil(std::log2(liftGameProcessedData.numGroups));
  liftGameProcessedData.indexShares.resize(groupWidth);
}

template <int schedulerId>
inline void computeTestIndexShares(
    LiftGameProcessedData<schedulerId>& liftGameProcessedData,
    const SecBit<schedulerId>& controlPopulation,
    const SecGroup<schedulerId>& testGroupIds) {
  // We only compute the reach metrics for the test population, hence we also
  // contruct index shares for just the test population. Similarly to how we
  // construct index shares in privatelyShareIndexSharesStep, we have to
  // differentiate the publisher breakdowns and partner cohorts when assigning
  // the group ids. There are now up to 2 * numPartnerCohorts_ + 1 group ids
  // in total, and we assign the first numPartnerCohorts_ to breakdown id 0, the
  // second numPartnerCohorts_ to breakdown id 1, and the last group id to the
  // control population.
  auto pubControlGroupId =
      common::createPublicBatchConstant<PubGroup<schedulerId>>(
          liftGameProcessedData.numTestGroups - 1,
          liftGameProcessedData.numRows);

  // We now set the group ids depending on whether each row is a test or
  // control
  auto secGroupIds = testGroupIds.mux(controlPopulation, pubControlGroupId);

  // Generate index shares from group ids
  liftGameProcessedData.testIndexShares =
      secGroupIds.extractIntShare().getBooleanShares();
  // Resize to width needed for the number of groups
  size_t testGroupWidth =
      std::ceil(std::log2(liftGameProcessedData.numTestGroups));
  liftGameProcessedData.testIndexShares.resize(testGroupWidth);
}

} // namespace private_lift::input_processing
