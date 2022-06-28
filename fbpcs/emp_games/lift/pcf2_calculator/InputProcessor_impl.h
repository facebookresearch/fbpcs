/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

namespace private_lift {

template <int schedulerId>
void InputProcessor<schedulerId>::validateNumRowsStep() {
  XLOG(INFO) << "Share number of rows";
  const size_t width = 32;
  auto publisherNumRows = common::
      shareIntFrom<schedulerId, width, common::PUBLISHER, common::PARTNER>(
          myRole_, numRows_);
  auto partnerNumRows = common::
      shareIntFrom<schedulerId, width, common::PARTNER, common::PUBLISHER>(
          myRole_, numRows_);

  if (publisherNumRows != partnerNumRows) {
    XLOG(ERR) << "The publisher has " << publisherNumRows
              << " rows in their input, while the partner has "
              << partnerNumRows << " rows.";
    exit(1);
  }
}

template <int schedulerId>
void InputProcessor<schedulerId>::privatelySharePopulationStep() {
  XLOG(INFO) << "Share control population";
  controlPopulation_ = common::privatelyShareArrayWithPaddingFrom<
      common::PUBLISHER,
      bool,
      SecBit<schedulerId>>(inputData_.getControlPopulation(), numRows_, 0);
}

template <int schedulerId>
void InputProcessor<schedulerId>::shareNumGroupsStep() {
  // TODO: We shouldn't be using MPC for this, it should just be shared over
  // a normal network socket as part of the protocol setup
  XLOG(INFO) << "Set up number of partner groups";
  if (inputData_.getNumGroups() >
      (1
       << (groupWidth - 1))) { // subtract one because we multiply the number of
    // groups by 2 for the test/control populations
    XLOG(ERR) << "The input has " << inputData_.getNumGroups()
              << " groups but we only support " << (1 << groupWidth)
              << " groups.";
    exit(1);
  }
  numPartnerCohorts_ = common::
      shareIntFrom<schedulerId, groupWidth, common::PARTNER, common::PUBLISHER>(
          myRole_, inputData_.getNumGroups());
  numPublisherBreakdowns_ = common::
      shareIntFrom<schedulerId, groupWidth, common::PUBLISHER, common::PARTNER>(
          myRole_, inputData_.getNumGroups());
  if (numPublisherBreakdowns_ > 2) {
    XLOG(ERR)
        << "The input has " << numPublisherBreakdowns_
        << " publisher breakdowns but we only support 2 publisher breakdowns.";
    exit(1);
  }
  // The number of groups is 2 (for test/control population) times the number of
  // partner cohorts and the number of publisher breakdowns. If there are no
  // cohorts or breakdowns, we multiply by 1 instead.
  numGroups_ = 2 * std::max(uint32_t(1), numPartnerCohorts_) *
      std::max(uint32_t(1), numPublisherBreakdowns_);
  // The test groups consist of the groups corresponding to the test population,
  // and one additional group for the control population (disregarding
  // breakdown or cohort id). These are used for computing reach metrics, which
  // are only for the test population.
  numTestGroups_ = 1 + numGroups_ / 2;
  XLOG(INFO) << "Will be computing metrics for " << numPublisherBreakdowns_
             << " publisher breakdowns and " << numPartnerCohorts_
             << " partner cohorts";
}

template <int schedulerId>
void InputProcessor<schedulerId>::privatelyShareGroupIdsStep() {
  XLOG(INFO) << "Share cohort group ids";
  cohortGroupIds_ = common::privatelyShareArrayWithPaddingFrom<
      common::PARTNER,
      uint32_t,
      SecGroup<schedulerId>>(inputData_.getGroupIds(), numRows_, 0);

  XLOG(INFO) << "Share publisher breakdown group ids";
  std::vector<bool> booleanBreakdownGroupIds(
      inputData_.getBreakdownIds().begin(), inputData_.getBreakdownIds().end());
  breakdownGroupIds_ = common::privatelyShareArrayWithPaddingFrom<
      common::PUBLISHER,
      bool,
      SecBit<schedulerId>>(booleanBreakdownGroupIds, numRows_, 0);
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
  if (numPartnerCohorts_ > 0) {
    for (auto groupId : inputData_.getGroupIds()) {
      testBreakdown1GroupIds.push_back(groupId + numPartnerCohorts_);
      controlBreakdown0GroupIds.push_back(groupId + 2 * numPartnerCohorts_);
      controlBreakdown1GroupIds.push_back(groupId + 3 * numPartnerCohorts_);
    }
  }

  // We share the new group ids with padding values 1, 2 and 3, to account for
  // the case where there are no cohorts.
  auto secTestBreakdown1GroupIds = common::privatelyShareArrayWithPaddingFrom<
      common::PARTNER,
      uint32_t,
      SecGroup<schedulerId>>(testBreakdown1GroupIds, numRows_, 1);
  auto secControlBreakdown0GroupIds =
      common::privatelyShareArrayWithPaddingFrom<
          common::PARTNER,
          uint32_t,
          SecGroup<schedulerId>>(
          controlBreakdown0GroupIds,
          numRows_,
          numPublisherBreakdowns_ > 0 ? 2 : 1);
  auto secControlBreakdown1GroupIds =
      common::privatelyShareArrayWithPaddingFrom<
          common::PARTNER,
          uint32_t,
          SecGroup<schedulerId>>(controlBreakdown1GroupIds, numRows_, 3);

  // We now set the group ids depending on whether each row is a test or
  // control, and whether the breakdown id is 0 or 1.
  testGroupIds_ =
      cohortGroupIds_.mux(breakdownGroupIds_, secTestBreakdown1GroupIds);
  auto secControlGroupIds = secControlBreakdown0GroupIds.mux(
      breakdownGroupIds_, secControlBreakdown1GroupIds);
  auto secGroupIds = testGroupIds_.mux(controlPopulation_, secControlGroupIds);

  // Generate index shares from group ids
  indexShares_ = secGroupIds.extractIntShare().getBooleanShares();
  // Resize to width needed for the number of groups
  size_t groupWidth = std::ceil(std::log2(numGroups_));
  indexShares_.resize(groupWidth);
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
  std::vector<uint32_t> controlGroupIds(numRows_, numTestGroups_ - 1);

  // We share the new control group ids
  auto secControlGroupIds = common::privatelyShareArrayWithPaddingFrom<
      common::PARTNER,
      uint32_t,
      SecGroup<schedulerId>>(controlGroupIds, numRows_, numTestGroups_ - 1);

  // We now set the group ids depending on whether each row is a test or
  // control
  auto secGroupIds = testGroupIds_.mux(controlPopulation_, secControlGroupIds);

  // Generate index shares from group ids
  testIndexShares_ = secGroupIds.extractIntShare().getBooleanShares();
  // Resize to width needed for the number of groups
  size_t testGroupWidth = std::ceil(std::log2(numTestGroups_));
  testIndexShares_.resize(testGroupWidth);
}

template <int schedulerId>
void InputProcessor<schedulerId>::privatelyShareTimestampsStep() {
  // TODO: We're using 32 bits for timestamps along with an offset setting the
  // epoch to 2019-01-01. This will break in the year 2087.
  XLOG(INFO) << "Share opportunity timestamps";
  opportunityTimestamps_ = common::privatelyShareArrayWithPaddingFrom<
      common::PUBLISHER,
      uint32_t,
      SecTimestamp<schedulerId>>(
      inputData_.getOpportunityTimestamps(), numRows_, 0);

  XLOG(INFO) << "Share if opportunity timestamps are valid";
  std::vector<bool> isValidOpportunityTimestamp;
  for (size_t i = 0; i < inputData_.getOpportunityTimestamps().size(); ++i) {
    // Nonzero opportunity timestamp and is opportunity (test or control)
    isValidOpportunityTimestamp.push_back(
        (inputData_.getOpportunityTimestamps().at(i) > 0) &
        (inputData_.getControlPopulation().at(i) |
         inputData_.getTestPopulation().at(i)));
  }
  isValidOpportunityTimestamp_ = common::privatelyShareArrayWithPaddingFrom<
      common::PUBLISHER,
      bool,
      SecBit<schedulerId>>(isValidOpportunityTimestamp, numRows_, 0);

  XLOG(INFO) << "Share purchase timestamps";
  purchaseTimestamps_ = common::privatelyShareTransposedArraysWithPaddingFrom<
      common::PARTNER,
      uint32_t,
      SecTimestamp<schedulerId>>(
      inputData_.getPurchaseTimestampArrays(),
      numRows_,
      numConversionsPerUser_,
      0);

  XLOG(INFO) << "Share if any purchase timestamp is valid";
  std::vector<bool> anyValidPurchaseTimestamp;
  for (auto& purchaseTimestampArray : inputData_.getPurchaseTimestampArrays()) {
    bool anyValidPurchaseTs = false;
    for (auto purchaseTs : purchaseTimestampArray) {
      // compute whether each row contains at least one valid (positive)
      // purchase timestamp
      anyValidPurchaseTs = anyValidPurchaseTs | (purchaseTs > 0);
    }
    anyValidPurchaseTimestamp.push_back(anyValidPurchaseTs);
  }
  anyValidPurchaseTimestamp_ = common::privatelyShareArrayWithPaddingFrom<
      common::PARTNER,
      bool,
      SecBit<schedulerId>>(anyValidPurchaseTimestamp, numRows_, 0);

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
  thresholdTimestamps_ = common::privatelyShareTransposedArraysWithPaddingFrom<
      common::PARTNER,
      uint32_t,
      SecTimestamp<schedulerId>>(
      thresholdTimestampArrays, numRows_, numConversionsPerUser_, 0);
}

template <int schedulerId>
void InputProcessor<schedulerId>::privatelySharePurchaseValuesStep() {
  XLOG(INFO) << "Share purchase values";
  // Since the input values are processed row by row, while we will be doing
  // batch computations with the values across the rows, we have to first
  // transpose the input arrays before sharing them in MPC.
  purchaseValues_ = common::privatelyShareTransposedArraysWithPaddingFrom<
      common::PARTNER,
      int64_t,
      SecValue<schedulerId>>(
      inputData_.getPurchaseValueArrays(), numRows_, numConversionsPerUser_, 0);

  XLOG(INFO) << "Share purchase values squared";
  purchaseValueSquared_ = common::privatelyShareTransposedArraysWithPaddingFrom<
      common::PARTNER,
      int64_t,
      SecValueSquared<schedulerId>>(
      inputData_.getPurchaseValueSquaredArrays(),
      numRows_,
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
  testReach_ = common::privatelyShareArrayWithPaddingFrom<
      common::PUBLISHER,
      bool,
      SecBit<schedulerId>>(testReach, numRows_, 0);
}

} // namespace private_lift
