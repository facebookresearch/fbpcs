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
void Attributor<
    schedulerId>::calculateNumConvSquaredAndValueSquaredAndConverters() {
  XLOG(INFO) << "Calculate numConvSquared & valueSquared & converters";
  if (events_.size() != inputProcessor_.getPurchaseValueSquared().size()) {
    XLOG(FATAL)
        << "Numbers of event bits and purchase values squared are inconsistent.";
  }
  // We find the first valid event using a binary tree approach. The number of
  // conversions is the remaining number of elements in the array. The value
  // squared is the squared sum of the values for valid events in each row.
  // These sums are already precomputed, so it suffices to check for the first
  // valid event and use the sum corresponding to the position in the row.

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

  std::vector<SecValueSquared<schedulerId>> valueSquaredArray =
      inputProcessor_.getPurchaseValueSquared();
  // The value squared is zero if there are no valid events
  SecValueSquared<schedulerId> zeroValueSquared{
      std::vector<int64_t>(numRows_, 0), common::PUBLISHER};
  valueSquaredArray.push_back(zeroValueSquared);

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
        // The same logic applies for the valueSquared
        valueSquaredArray[i + stepSize] =
            valueSquaredArray.at(i + stepSize)
                .mux(eventArray.at(i), valueSquaredArray.at(i));
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
        valueSquaredArray[previousIndex] = valueSquaredArray.at(i).mux(
            eventArray.at(previousIndex), valueSquaredArray.at(previousIndex));
        eventArray[previousIndex] =
            eventArray.at(previousIndex) | eventArray.at(i);
      }
    }
    firstIndex += stepSize;
    stepSize = stepSize << 1;
  }
  valueSquared_ = valueSquaredArray.at(firstIndex);
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

template <int schedulerId>
void Attributor<schedulerId>::calculateReachedConversions() {
  XLOG(INFO) << "Calculate reached conversions";
  for (const auto& event : events_) {
    // A reached conversion is when there is a reach (number of impressions > 0)
    // and a valid event, and this is only calculated for the test population
    reachedConversions_.push_back(
        std::move(event & inputProcessor_.getTestReach()));
  }
}

template <int schedulerId>
void Attributor<schedulerId>::calculateValues() {
  XLOG(INFO) << "Calculate values";
  if (events_.size() != inputProcessor_.getPurchaseValues().size()) {
    XLOG(FATAL)
        << "Numbers of event bits and/or purchase values are inconsistent.";
  }
  auto zero = PubValue<schedulerId>(std::vector<int64_t>(numRows_, 0));
  for (size_t i = 0; i < events_.size(); ++i) {
    // The value is the purchase value if there is a valid event, otherwise it
    // is zero
    values_.push_back(std::move(
        zero.mux(events_.at(i), inputProcessor_.getPurchaseValues().at(i))));
  }

  XLOG(INFO) << "Calculate reached values";
  // A reached value is the value when there is a reach, otherwise it is zero.
  // This is only calculated for the test population.
  for (const auto& value : values_) {
    reachedValues_.push_back(
        std::move(zero.mux(inputProcessor_.getTestReach(), value)));
  }
}

} // namespace private_lift
