/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <gflags/gflags_declare.h>

DECLARE_int32(party);
DECLARE_string(server_ip);
DECLARE_int32(port);
DECLARE_string(input_base_path);
DECLARE_string(output_base_path);
DECLARE_int32(file_start_index);
DECLARE_int32(num_files);
DECLARE_string(attribution_rules);
DECLARE_string(aggregators);
DECLARE_int32(concurrency);
DECLARE_bool(use_xor_encryption);
DECLARE_string(run_name);
DECLARE_bool(use_postfix);
DECLARE_int32(max_num_touchpoints);
DECLARE_int32(max_num_conversions);
DECLARE_int32(input_encryption);
DECLARE_bool(log_cost);
DECLARE_string(log_cost_s3_bucket);
DECLARE_string(log_cost_s3_region);
