/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "fbpcs/emp_games/data_processing/unified_data_process/UdpProcessOptions.h"

DEFINE_int32(party, 1, "1 = publisher, 2 = partner");
DEFINE_bool(
    use_xor_encryption,
    true,
    "Reveal output with XOR secret shares instead of in the clear to both parties");
DEFINE_string(server_ip, "127.0.0.1", "Server's IP Address");
DEFINE_int32(
    port,
    10000,
    "Network port for establishing connection to other player");

// UDP settings
DEFINE_int64(row_number, 1000000, "Number of input rows");
DEFINE_int64(row_size, 1000000, "Number of input rows");
DEFINE_int64(intersection, 150000, "Size of intersection");

// Logging flags
DEFINE_string(
    run_name,
    "",
    "A user given run name that will be used in s3 filename");
DEFINE_bool(
    log_cost,
    false,
    "Log cost info into cloud which will be used for dashboard");
DEFINE_string(log_cost_s3_bucket, "cost-estimation-logs", "s3 bucket name");
DEFINE_string(
    log_cost_s3_region,
    ".s3.us-west-2.amazonaws.com/",
    "s3 region name");

DEFINE_string(
    pc_feature_flags,
    "",
    "A String of PC Feature Flags passing from PCS, separated by comma");
