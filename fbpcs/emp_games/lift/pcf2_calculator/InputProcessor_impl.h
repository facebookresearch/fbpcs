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
void InputProcessor<schedulerId>::privatelyShareCohortsStep() {
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
  XLOG(INFO) << "Will be computing metrics for " << numPartnerCohorts_
             << " partner cohorts";

  // We compute the metrics for both test/control populations and cohorts. To
  // differentiate the cohort group ids for the test/control population, we set
  // the test group ids as the original group ids, and the control group ids as
  // the original group ids plus numPartnerCohorts_. If there are no partner
  // cohorts, the original group ids are all zero, and we set the control group
  // ids to be 1 to differentiate them from the test group ids.
  cohortGroupIds_ = common::privatelyShareArrayWithPaddingFrom<
      common::PARTNER,
      uint32_t,
      SecGroup<schedulerId>>(inputData_.getGroupIds(), numRows_, 0);
  std::vector<uint32_t> controlGroupIds;
  if (numPartnerCohorts_ > 0) {
    for (auto groupId : inputData_.getGroupIds()) {
      controlGroupIds.push_back(groupId + numPartnerCohorts_);
    }
  }
  // We set the padding value for the control group ids to be 1, so that if
  // there are no cohorts, the control group ids are all 1.
  auto secControlGroupIds = common::privatelyShareArrayWithPaddingFrom<
      common::PARTNER,
      uint32_t,
      SecGroup<schedulerId>>(controlGroupIds, numRows_, 1);
  // We now set the group ids depending on whether each row is a test or
  // control
  auto groupIds = cohortGroupIds_.mux(controlPopulation_, secControlGroupIds);
  cohortIndexShares_ = groupIds.extractIntShare().getBooleanShares();
  // Resize to width needed for the number of groups
  size_t cohortWidth =
      std::ceil(std::log2(std::max(uint32_t(2), 2 * numPartnerCohorts_)));
  cohortIndexShares_.resize(cohortWidth);
}

template <int schedulerId>
void InputProcessor<schedulerId>::privatelyShareTestCohortsStep() {
  // We only compute the reach metrics for the test population, hence we also
  // contruct cohort index shares for just the test population. To differentiate
  // the cohort group ids for the test/control population, we set the test group
  // ids as the original group ids, and the control group ids as
  // numPartnerCohorts_ (or 1 if there are no cohorts).
  std::vector<uint32_t> controlGroupIds(
      numRows_, std::max(uint32_t(1), numPartnerCohorts_));
  auto secControlGroupIds = common::privatelyShareArrayWithPaddingFrom<
      common::PARTNER,
      uint32_t,
      SecGroup<schedulerId>>(controlGroupIds, numRows_, 1);
  // We now set the group ids depending on whether each row is a test or
  // control
  auto groupIds = cohortGroupIds_.mux(controlPopulation_, secControlGroupIds);
  testCohortIndexShares_ = groupIds.extractIntShare().getBooleanShares();
  // Resize to width needed for the number of groups
  size_t testCohortWidth =
      std::ceil(std::log2(std::max(uint32_t(2), numPartnerCohorts_ + 1)));
  testCohortIndexShares_.resize(testCohortWidth);
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
