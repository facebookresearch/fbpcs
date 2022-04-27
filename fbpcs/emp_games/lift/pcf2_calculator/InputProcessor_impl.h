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
