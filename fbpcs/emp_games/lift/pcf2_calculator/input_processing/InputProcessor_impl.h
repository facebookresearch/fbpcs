/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include "fbpcs/emp_games/lift/pcf2_calculator/input_processing/InputProcessor.h"

namespace private_lift {

template <int schedulerId>
void InputProcessor<schedulerId>::privatelyShareGroupIdsStep() {
  XLOG(INFO) << "Share cohort group ids";
  cohortGroupIds_ = common::privatelyShareArrayWithPaddingFrom<
      common::PARTNER,
      uint32_t,
      SecGroup<schedulerId>>(
      inputData_.getGroupIds(), liftGameProcessedData_.numRows, 0);

  XLOG(INFO) << "Share publisher breakdown group ids";
  std::vector<bool> booleanBreakdownGroupIds(
      inputData_.getBreakdownIds().begin(), inputData_.getBreakdownIds().end());
  breakdownGroupIds_ = common::privatelyShareArrayWithPaddingFrom<
      common::PUBLISHER,
      bool,
      SecBit<schedulerId>>(
      booleanBreakdownGroupIds, liftGameProcessedData_.numRows, 0);
}

template <int schedulerId>
void InputProcessor<schedulerId>::privatelySharePopulationStep() {
  XLOG(INFO) << "Share control population";
  controlPopulation_ = common::privatelyShareArrayWithPaddingFrom<
      common::PUBLISHER,
      bool,
      SecBit<schedulerId>>(
      inputData_.getControlPopulation(), liftGameProcessedData_.numRows, 0);
}

template <int schedulerId>
void InputProcessor<schedulerId>::privatelyShareIndexSharesStep() {
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
  std::vector<uint32_t> testBreakdown1GroupIds;
  std::vector<uint32_t> controlBreakdown0GroupIds;
  std::vector<uint32_t> controlBreakdown1GroupIds;
  if (liftGameProcessedData_.numPartnerCohorts > 0) {
    for (auto groupId : inputData_.getGroupIds()) {
      if (liftGameProcessedData_.numPublisherBreakdowns > 0) {
        testBreakdown1GroupIds.push_back(
            groupId + liftGameProcessedData_.numPartnerCohorts);
        controlBreakdown0GroupIds.push_back(
            groupId + 2 * liftGameProcessedData_.numPartnerCohorts);
        controlBreakdown1GroupIds.push_back(
            groupId + 3 * liftGameProcessedData_.numPartnerCohorts);
      } else {
        controlBreakdown0GroupIds.push_back(
            groupId + liftGameProcessedData_.numPartnerCohorts);
      }
    }
  }

  SecGroup<schedulerId> secControlGroupIds;
  if (liftGameProcessedData_.numPublisherBreakdowns > 0) {
    // We share the new group ids with padding values 1, 2 and 3, to account for
    // the case where there are no cohorts.
    auto secTestBreakdown1GroupIds = common::privatelyShareArrayWithPaddingFrom<
        common::PARTNER,
        uint32_t,
        SecGroup<schedulerId>>(
        testBreakdown1GroupIds, liftGameProcessedData_.numRows, 1);
    auto secControlBreakdown0GroupIds =
        common::privatelyShareArrayWithPaddingFrom<
            common::PARTNER,
            uint32_t,
            SecGroup<schedulerId>>(
            controlBreakdown0GroupIds, liftGameProcessedData_.numRows, 2);
    auto secControlBreakdown1GroupIds =
        common::privatelyShareArrayWithPaddingFrom<
            common::PARTNER,
            uint32_t,
            SecGroup<schedulerId>>(
            controlBreakdown1GroupIds, liftGameProcessedData_.numRows, 3);

    // We now set the group ids depending on whether each row is a test or
    // control, and whether the breakdown id is 0 or 1.
    testGroupIds_ =
        cohortGroupIds_.mux(breakdownGroupIds_, secTestBreakdown1GroupIds);
    secControlGroupIds = secControlBreakdown0GroupIds.mux(
        breakdownGroupIds_, secControlBreakdown1GroupIds);
  } else {
    testGroupIds_ = cohortGroupIds_;
    secControlGroupIds = common::privatelyShareArrayWithPaddingFrom<
        common::PARTNER,
        uint32_t,
        SecGroup<schedulerId>>(
        controlBreakdown0GroupIds, liftGameProcessedData_.numRows, 1);
  }

  auto secGroupIds = testGroupIds_.mux(controlPopulation_, secControlGroupIds);
  // Generate index shares from group ids
  liftGameProcessedData_.indexShares =
      secGroupIds.extractIntShare().getBooleanShares();
  // Resize to width needed for the number of groups
  size_t groupWidth = std::ceil(std::log2(liftGameProcessedData_.numGroups));
  liftGameProcessedData_.indexShares.resize(groupWidth);
}

template <int schedulerId>
void InputProcessor<schedulerId>::privatelyShareTestIndexSharesStep() {
  // We only compute the reach metrics for the test population, hence we also
  // contruct index shares for just the test population. Similarly to how we
  // construct index shares in privatelyShareIndexSharesStep, we have to
  // differentiate the publisher breakdowns and partner cohorts when assigning
  // the group ids. There are now up to 2 * numPartnerCohorts_ + 1 group ids
  // in total, and we assign the first numPartnerCohorts_ to breakdown id 0, the
  // second numPartnerCohorts_ to breakdown id 1, and the last group id to the
  // control population.
  std::vector<uint32_t> controlGroupIds(
      liftGameProcessedData_.numRows, liftGameProcessedData_.numTestGroups - 1);

  // We share the new control group ids
  auto secControlGroupIds = common::privatelyShareArrayWithPaddingFrom<
      common::PARTNER,
      uint32_t,
      SecGroup<schedulerId>>(
      controlGroupIds,
      liftGameProcessedData_.numRows,
      liftGameProcessedData_.numTestGroups - 1);

  // We now set the group ids depending on whether each row is a test or
  // control
  auto secGroupIds = testGroupIds_.mux(controlPopulation_, secControlGroupIds);

  // Generate index shares from group ids
  liftGameProcessedData_.testIndexShares =
      secGroupIds.extractIntShare().getBooleanShares();
  // Resize to width needed for the number of groups
  size_t testGroupWidth =
      std::ceil(std::log2(liftGameProcessedData_.numTestGroups));
  liftGameProcessedData_.testIndexShares.resize(testGroupWidth);
}

template <int schedulerId>
void InputProcessor<schedulerId>::privatelyShareTimestampsStep() {
  // TODO: We're using 32 bits for timestamps along with an offset setting the
  // epoch to 2019-01-01. This will break in the year 2087.
  XLOG(INFO) << "Share opportunity timestamps";
  liftGameProcessedData_.opportunityTimestamps =
      common::privatelyShareArrayWithPaddingFrom<
          common::PUBLISHER,
          uint32_t,
          SecTimestamp<schedulerId>>(
          inputData_.getOpportunityTimestamps(),
          liftGameProcessedData_.numRows,
          0);

  XLOG(INFO) << "Share if opportunity timestamps are valid";
  std::vector<bool> isValidOpportunityTimestamp;
  for (size_t i = 0; i < inputData_.getOpportunityTimestamps().size(); ++i) {
    // Nonzero opportunity timestamp and is opportunity (test or control)
    isValidOpportunityTimestamp.push_back(
        (inputData_.getOpportunityTimestamps().at(i) > 0) &
        (inputData_.getControlPopulation().at(i) |
         inputData_.getTestPopulation().at(i)));
  }
  liftGameProcessedData_.isValidOpportunityTimestamp =
      common::privatelyShareArrayWithPaddingFrom<
          common::PUBLISHER,
          bool,
          SecBit<schedulerId>>(
          isValidOpportunityTimestamp, liftGameProcessedData_.numRows, 0);

  XLOG(INFO) << "Share purchase timestamps";
  liftGameProcessedData_.purchaseTimestamps =
      common::privatelyShareTransposedArraysWithPaddingFrom<
          common::PARTNER,
          uint32_t,
          SecTimestamp<schedulerId>>(
          inputData_.getPurchaseTimestampArrays(),
          liftGameProcessedData_.numRows,
          numConversionsPerUser_,
          0);

  XLOG(INFO) << "Share if any purchase timestamp is valid";
  std::vector<bool> anyValidPurchaseTimestamp;
  for (const std::vector<uint32_t>& purchaseTimestampArray :
       inputData_.getPurchaseTimestampArrays()) {
    bool anyValidPurchaseTs = false;
    for (uint32_t purchaseTs : purchaseTimestampArray) {
      // compute whether each row contains at least one valid (positive)
      // purchase timestamp
      anyValidPurchaseTs = anyValidPurchaseTs | (purchaseTs > 0);
    }
    anyValidPurchaseTimestamp.push_back(anyValidPurchaseTs);
  }
  liftGameProcessedData_.anyValidPurchaseTimestamp =
      common::privatelyShareArrayWithPaddingFrom<
          common::PARTNER,
          bool,
          SecBit<schedulerId>>(
          anyValidPurchaseTimestamp, liftGameProcessedData_.numRows, 0);

  XLOG(INFO) << "Share threshold timestamps";
  // Threshold timestamps are valid (positive) purchase timestamp with added
  // attribution window
  const int window = 10;
  std::vector<std::vector<uint32_t>> thresholdTimestampArrays;
  for (const auto& purchaseTimestampArray :
       inputData_.getPurchaseTimestampArrays()) {
    std::vector<uint32_t> thresholdTimestampArray;
    for (auto purchaseTimestamp : purchaseTimestampArray) {
      auto thresholdTimestamp =
          purchaseTimestamp > 0 ? purchaseTimestamp + window : 0;
      thresholdTimestampArray.push_back(thresholdTimestamp);
    }
    thresholdTimestampArrays.push_back(std::move(thresholdTimestampArray));
  }
  // Secretly share threshold timestamps
  liftGameProcessedData_.thresholdTimestamps =
      common::privatelyShareTransposedArraysWithPaddingFrom<
          common::PARTNER,
          uint32_t,
          SecTimestamp<schedulerId>>(
          thresholdTimestampArrays,
          liftGameProcessedData_.numRows,
          numConversionsPerUser_,
          0);
}

template <int schedulerId>
void InputProcessor<schedulerId>::privatelySharePurchaseValuesStep() {
  XLOG(INFO) << "Share purchase values";
  // Since the input values are processed row by row, while we will be doing
  // batch computations with the values across the rows, we have to first
  // transpose the input arrays before sharing them in MPC.
  liftGameProcessedData_.purchaseValues =
      common::privatelyShareTransposedArraysWithPaddingFrom<
          common::PARTNER,
          int64_t,
          SecValue<schedulerId>>(
          inputData_.getPurchaseValueArrays(),
          liftGameProcessedData_.numRows,
          numConversionsPerUser_,
          0);

  XLOG(INFO) << "Share purchase values squared";
  liftGameProcessedData_.purchaseValueSquared =
      common::privatelyShareTransposedArraysWithPaddingFrom<
          common::PARTNER,
          int64_t,
          SecValueSquared<schedulerId>>(
          inputData_.getPurchaseValueSquaredArrays(),
          liftGameProcessedData_.numRows,
          numConversionsPerUser_,
          0);
}

template <int schedulerId>
void InputProcessor<schedulerId>::privatelyShareTestReachStep() {
  XLOG(INFO) << "Share reach";
  std::vector<bool> testReach;
  for (size_t i = 0; i < inputData_.getNumImpressions().size(); ++i) {
    // A reach occurs when the number of impressions is nonzero, and we only
    // compute this for the test population.
    testReach.push_back(
        inputData_.getTestPopulation().at(i) &
        (inputData_.getNumImpressions().at(i) > 0));
  }
  liftGameProcessedData_.testReach = common::privatelyShareArrayWithPaddingFrom<
      common::PUBLISHER,
      bool,
      SecBit<schedulerId>>(testReach, liftGameProcessedData_.numRows, 0);
}

} // namespace private_lift
