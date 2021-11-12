/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "fbpcs/emp_games/lift/common/ColumnNameConstants.h"

#include <string>

namespace private_lift::lift_columns {
// Publisher columns
const std::string kOpportunityTimestamp{"opportunity_timestamp"};
const std::string kPopulation{"population"};
const std::string kNumImpressions{"num_impressions"};
const std::string kReached{"reached"};
const std::string kNumClicks{"num_clicks"};
const std::string kTotalSpend{"total_spend"};
const std::string kBreakdownId{"breakdown_id"};

// Partner columns
const std::string kPartnerRow{"partner_row"};
const std::string kEventTimestamps{"event_timestamps"};
const std::string kValues{"values"};
const std::string kValuesSquared{"values_squared"};
const std::string kCohortId{"cohort_id"};

// Derived columns
const std::string kValidConversions{"valid_conversions"};
const std::string kConverters{"converters"};
const std::string kUserValue{"user_value"};
const std::string kUserValueSquared{"user_value_squared"};
const std::string kUserNumConvSquared{"user_num_conv_squared"};
const std::string kMatchCount{"match_count"};
const std::string kReachedConversions{"reached_conversions"};
const std::string kReachedValue{"reached_value"};
const std::string kConvHistogram{"conv_histogram"};
} // namespace private_lift::lift_columns
