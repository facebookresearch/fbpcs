/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <fbpcs/emp_games/common/Constants.h>
#include <fbpcs/emp_games/pcf2_shard_combiner/AggMetrics.h>

namespace shard_combiner {

enum class ShardSchemaType { kTest, kAdObjFormat, kGroupedLiftMetrics };

template <
    ShardSchemaType shardSchemaType,
    int schedulerId,
    bool usingBatch,
    common::InputEncryption inputEncryption>
void validateShardSchema(
    const AggMetrics<schedulerId, usingBatch, inputEncryption>& metrics);

} // namespace shard_combiner
