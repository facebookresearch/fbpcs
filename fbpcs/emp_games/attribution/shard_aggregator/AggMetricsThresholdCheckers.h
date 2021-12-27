/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include "fbpcs/emp_games/attribution/shard_aggregator/AggMetrics.h"

namespace measurement::private_attribution {
std::function<void(std::shared_ptr<private_measurement::AggMetrics>)>
constructLiftThresholdChecker(int64_t threshold);
std::function<void(std::shared_ptr<private_measurement::AggMetrics>)>
constructAdObjectFormatThresholdChecker(int64_t threshold);
} // namespace measurement::private_attribution
