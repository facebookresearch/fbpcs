/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <string>

namespace private_lift::lift_columns {
// Publisher columns
extern const std::string kOpportunityTimestamp;
extern const std::string kTestPopulation;
extern const std::string kControlPopulation;
extern const std::string kNumImpressions;
extern const std::string kReached;
extern const std::string kNumClicks;
extern const std::string kTotalSpend;
extern const std::string kBreakdownId;

// Partner columns
extern const std::string kPartnerRow;
extern const std::string kEventTimestamps;
extern const std::string kValues;
extern const std::string kValuesSquared;
extern const std::string kCohortId;

// Derived columns
extern const std::string kValidConversions;
extern const std::string kConverters;
extern const std::string kUserValue;
extern const std::string kUserValueSquared;
extern const std::string kUserNumConvSquared;
extern const std::string kMatchCount;
extern const std::string kReachedConversions;
extern const std::string kReachedValue;
extern const std::string kConvHistogram;
} // namespace private_lift::lift_columns
