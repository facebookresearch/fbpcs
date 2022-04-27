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
