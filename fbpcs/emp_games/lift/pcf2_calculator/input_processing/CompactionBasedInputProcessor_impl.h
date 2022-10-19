/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <algorithm>
#include <stdexcept>
#include "fbpcs/emp_games/lift/pcf2_calculator/input_processing/CompactionBasedInputProcessor.h"
#include "fbpcs/emp_games/lift/pcf2_calculator/input_processing/IInputProcessor.h"

namespace private_lift {

template <int schedulerId>
std::vector<int32_t>
CompactionBasedInputProcessor<schedulerId>::shuffleAndGetUnionMap() {
  // dummy implementation. Assume each PID is a match
  std::vector<int32_t> unionMap(inputData_.getNumRows());
  for (int32_t i = 0; i < unionMap.size(); i++) {
    unionMap[i] = i;
  }
  return unionMap;
}

template <int schedulerId>
std::vector<int32_t>
CompactionBasedInputProcessor<schedulerId>::getIntersectionMap(
    const std::vector<int32_t>& unionMap) {
  return adapter_->adapt(unionMap);
}

template <int schedulerId>
std::vector<std::vector<unsigned char>>
CompactionBasedInputProcessor<schedulerId>::preparePlaintextData() {
  throw std::runtime_error("Not implemented");
}

template <int schedulerId>
void CompactionBasedInputProcessor<schedulerId>::compactData(
    const std::vector<int32_t>& intersectionMap,
    const std::vector<std::vector<unsigned char>>& plaintextData) {
  int32_t myDataWidth = plaintextData[0].size();

  auto publisherDataSize = common::shareIntFrom<
      schedulerId,
      sizeof(myDataWidth) * 8,
      common::PUBLISHER,
      common::PARTNER>(myRole_, myDataWidth);

  auto partnerDataSize = common::shareIntFrom<
      schedulerId,
      sizeof(myDataWidth) * 8,
      common::PARTNER,
      common::PUBLISHER>(myRole_, myDataWidth);

  if (myRole_ == common::PUBLISHER) {
    publisherDataShares_ =
        dataProcessor_->processMyData(plaintextData, intersectionMap.size());
    partnerDataShares_ = dataProcessor_->processPeersData(
        inputData_.getNumRows(), intersectionMap, partnerDataSize);
  } else if (myRole_ == common::PARTNER) {
    publisherDataShares_ = dataProcessor_->processPeersData(
        inputData_.getNumRows(), intersectionMap, publisherDataSize);
    partnerDataShares_ =
        dataProcessor_->processMyData(plaintextData, intersectionMap.size());
  }
}

template <int schedulerId>
void CompactionBasedInputProcessor<schedulerId>::extractCompactedData() {
  throw std::runtime_error("Not implemented");
}

} // namespace private_lift
