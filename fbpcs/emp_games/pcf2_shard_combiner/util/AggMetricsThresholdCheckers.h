/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include "fbpcs/emp_games/pcf2_shard_combiner/AggMetrics.h"
#include "fbpcs/emp_games/pcf2_shard_combiner/ShardValidator.h"

namespace shard_combiner {

template <
    int32_t schedulerId,
    bool usingBatch = false,
    common::InputEncryption inputEncryption =
        common::InputEncryption::Plaintext>
using ThresholdFn = std::function<void(
    AggMetrics_sp<schedulerId, usingBatch, inputEncryption>)>;

// Returns a std::function that should replace metrics that fail to
// meet threshold with a sentinel value -1.
template <
    int schedulerId = 0,
    bool usingBatch = false,
    common::InputEncryption inputEncryption =
        common::InputEncryption::Plaintext>
ThresholdFn<schedulerId, usingBatch, inputEncryption>
checkThresholdAndUpdateMetric(
    ShardSchemaType shardSchemaType,
    int64_t threshold);
} // namespace shard_combiner
