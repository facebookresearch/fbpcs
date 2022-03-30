/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <gflags/gflags.h>

#include "fbpcs/emp_games/attribution/decoupled_attribution/AttributionOptions.h"

DEFINE_int32(party, 1, "1 = publisher, 2 = partner");
DEFINE_string(server_ip, "127.0.0.1", "Server's IP address");
DEFINE_int32(port, 5000, "Server's port");
DEFINE_string(
    input_base_path,
    "",
    "Local or s3 base path for the sharded input files");
DEFINE_string(
    output_base_path,
    "",
    "Local or s3 base path where output files are written to");
DEFINE_int32(
    file_start_index,
    0,
    "First file that will be read with base path");
DEFINE_int32(num_files, 0, "Number of files that should be read");
DEFINE_string(
    attribution_rules,
    "",
    "Comma separated list of attribution rules use. (Publisher Only)");
DEFINE_string(
    aggregators,
    "measurement",
    "Comma separated list of aggregators to use. (Publisher Only)");
DEFINE_int32(
    concurrency,
    1,
    "max number of game(s) that will run concurrently");
DEFINE_bool(
    use_xor_encryption,
    true,
    "Reveal output with XOR secret shares instead of in the clear to both parties");
DEFINE_string(
    run_name,
    "",
    "A user given run name that will be used in s3 filename");
DEFINE_bool(
    use_postfix,
    false,
    "A postfix number added to input/output files to accommodate sharding");
DEFINE_int32(max_num_touchpoints, 4, "Maximum touchpoints per user");
DEFINE_int32(max_num_conversions, 4, "Maximum conversions per user");
DEFINE_bool(
    log_cost,
    false,
    "Log cost info into cloud which will be used for dashboard");
