/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <gflags/gflags.h>

// MPC settings
DECLARE_int32(party);
DECLARE_bool(use_xor_encryption);
DECLARE_string(server_ip);
DECLARE_int32(port);

// UDP settings
DECLARE_int64(row_number);
DECLARE_int64(row_size);
DECLARE_int64(intersection);

// Logging flags
DECLARE_string(run_name);
DECLARE_bool(log_cost);
DECLARE_string(log_cost_s3_bucket);
DECLARE_string(log_cost_s3_region);

DECLARE_string(pc_feature_flags);
