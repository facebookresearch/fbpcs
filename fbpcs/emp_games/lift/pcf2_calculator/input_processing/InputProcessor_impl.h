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
  std::vector<std::vector<uint32_t>> thresholdTimestampArrays;
  for (const auto& purchaseTimestampArray :
       inputData_.getPurchaseTimestampArrays()) {
    std::vector<uint32_t> thresholdTimestampArray;
    for (auto purchaseTimestamp : purchaseTimestampArray) {
      auto thresholdTimestamp = purchaseTimestamp > 0
          ? purchaseTimestamp + kPurchaseTimestampThresholdWindow
          : 0;
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
