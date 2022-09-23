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

// Lift settings
DECLARE_string(input_path);
DECLARE_string(output_global_params_path);
DECLARE_string(output_secret_shares_path);
DECLARE_int32(epoch);
DECLARE_int32(num_conversions_per_user);
DECLARE_bool(compute_publisher_breakdowns);

DECLARE_string(pc_feature_flags);
