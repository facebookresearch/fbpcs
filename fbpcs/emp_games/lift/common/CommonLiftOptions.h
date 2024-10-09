/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <gflags/gflags_declare.h>

DECLARE_bool(compute_publisher_breakdowns);
DECLARE_bool(log_cost);
DECLARE_bool(use_tls);
DECLARE_bool(use_xor_encryption);
DECLARE_uint32(concurrency);
DECLARE_uint32(file_start_index);
DECLARE_uint32(num_conversions_per_user);
DECLARE_uint32(num_files);
DECLARE_uint32(party);
DECLARE_uint32(port);
DECLARE_uint64(epoch);
DECLARE_string(ca_cert_path);
DECLARE_string(input_base_path);
DECLARE_string(log_cost_s3_bucket);
DECLARE_string(log_cost_s3_region);
DECLARE_string(pc_feature_flags);
DECLARE_string(private_key_path);
DECLARE_string(run_name);
DECLARE_string(server_cert_path);
DECLARE_string(server_ip);
