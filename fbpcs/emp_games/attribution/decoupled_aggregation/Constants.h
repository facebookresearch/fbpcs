/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <emp-sh2pc/emp-sh2pc.h>
#include <string>

namespace aggregation::private_aggregation {

enum AGGREGATION_FORMAT { AD_OBJECT_FORMAT = 1 };

const int64_t INT_SIZE = 64;
const int64_t INT_SIZE_32 = 32;
const int64_t PUBLISHER = emp::ALICE;
const int64_t PARTNER = emp::BOB;

const int64_t INVALID_VALUE = -1;

} // namespace aggregation::private_aggregation
