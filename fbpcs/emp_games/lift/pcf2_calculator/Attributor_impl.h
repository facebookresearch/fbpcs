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

} // namespace private_lift
