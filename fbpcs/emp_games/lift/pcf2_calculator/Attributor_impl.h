/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

namespace private_lift {

template <int schedulerId>
void Attributor<schedulerId>::calculateEvents() {
  XLOG(INFO) << "Calculate events";
  for (auto& thresholdTs : inputProcessor_.getThresholdTimestamps()) {
    // Events occur when there is a valid purchase, i.e. the opportunity
    // timestamp is less than the threshold timestamp
    events_.push_back(std::move(
        inputProcessor_.getIsValidOpportunityTimestamp() &
        (thresholdTs > inputProcessor_.getOpportunityTimestamps())));
  }
}

template <int schedulerId>
void Attributor<schedulerId>::calculateNumConvSquaredAndConverters() {
  XLOG(INFO) << "Calculate numConvSquared & converters";
  // We find the first valid event using a binary tree approach. The number of
  // conversions is the remaining number of elements in the array.
  // We first construct new arrays to store the intermediate results
  std::vector<SecNumConvSquared<schedulerId>> numConvSquaredArray;
  for (size_t i = 0; i < events_.size(); ++i) {
    auto numConv = events_.size() - i;
    auto convSquared = static_cast<uint32_t>(numConv * numConv);
    SecNumConvSquared<schedulerId> numConvSquared(
        std::vector(numRows_, convSquared), common::PUBLISHER);
    numConvSquaredArray.push_back(numConvSquared);
  }
  // The numConvSquared is zero if there are no valid events
  SecNumConvSquared<schedulerId> zero{
      std::vector<uint32_t>(numRows_, 0), common::PUBLISHER};
  numConvSquaredArray.push_back(zero);

  std::vector<SecBit<schedulerId>> eventArray = events_;
  SecBit<schedulerId> zeroBit{
      std::vector<bool>(numRows_, false), common::PUBLISHER};
  eventArray.push_back(zeroBit);

  int stepSize = 1; // we process the array elements in pairs with indices
                    // differing by stepSize
  int firstIndex = 0; // first index at this level
  while (firstIndex < eventArray.size() / 2) {
    for (size_t i = firstIndex; i < eventArray.size(); i += 2 * stepSize) {
      if (i + stepSize < eventArray.size()) {
        // If there is a valid event at i, we set the numConvSquared to be
        // numConvSquared[i], else we set it to be numConvSquared[i + stepSize]
        numConvSquaredArray[i + stepSize] =
            numConvSquaredArray.at(i + stepSize)
                .mux(eventArray.at(i), numConvSquaredArray.at(i));
        // Update the events for the next level
        eventArray[i + stepSize] =
            eventArray.at(i + stepSize) | eventArray.at(i);
      } else {
        // Odd number of elements at this level, compute the mux with the
        // previous pair
        auto previousIndex = i - stepSize;
        numConvSquaredArray[previousIndex] = numConvSquaredArray.at(i).mux(
            eventArray.at(previousIndex),
            numConvSquaredArray.at(previousIndex));
        eventArray[previousIndex] =
            eventArray.at(previousIndex) | eventArray.at(i);
      }
    }
    firstIndex += stepSize;
    stepSize = stepSize << 1;
  }
  numConvSquared_ = numConvSquaredArray.at(firstIndex);
  // A converter occurs when a row contains any valid event
  converters_ = eventArray.at(firstIndex);
}

template <int schedulerId>
void Attributor<schedulerId>::calculateMatch() {
  XLOG(INFO) << "Calculate match";
  // a valid test/control match is when a person with an opportunity made
  // ANY nonzero conversion.
  match_ = inputProcessor_.getAnyValidPurchaseTimestamp() &
      inputProcessor_.getIsValidOpportunityTimestamp();
}

} // namespace private_lift
