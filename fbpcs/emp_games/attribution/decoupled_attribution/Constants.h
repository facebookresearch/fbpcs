/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <cstdint>

#include "fbpcs/emp_games/attribution/decoupled_attribution/Debug.h"

namespace aggregation::private_attribution {

const int64_t INT_SIZE = 64;
const int64_t TS_SIZE = INT_SIZE;
const int64_t PUBLISHER = emp::ALICE;
const int64_t PARTNER = emp::BOB;

const int64_t INVALID_TP_ID = -1;
} // namespace aggregation::private_attribution
