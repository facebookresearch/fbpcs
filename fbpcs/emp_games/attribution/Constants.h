/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include "Debug.h"
#include "fbpcs/emp_games/common/PrivateData.h"

namespace measurement::private_attribution {

const int64_t INT_SIZE = private_measurement::INT_SIZE;
const int64_t TS_SIZE = INT_SIZE;
const int64_t PUBLISHER = emp::ALICE;
const int64_t PARTNER = emp::BOB;

const int64_t INVALID_TP_ID = -1;
} // namespace measurement::private_attribution
