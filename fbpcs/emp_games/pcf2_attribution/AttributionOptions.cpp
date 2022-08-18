/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "fbpcs/emp_games/pcf2_attribution/AttributionOptions.h"
#include "fbpcs/emp_games/common/Constants.h"

#include <gflags/gflags.h>

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
    common::LAST_CLICK_1D,
    "Comma separated list of attribution rules use.");
DEFINE_string(
    aggregators,
    common::MEASUREMENT,
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
    true,
    "A postfix number added to input/output files to accommodate sharding");
DEFINE_int32(max_num_touchpoints, 4, "Maximum touchpoints per user");
DEFINE_int32(max_num_conversions, 4, "Maximum conversions per user");
DEFINE_int32(
    input_encryption,
    0,
    "0 for plaintext input, 1 for partner XOR encrypted input (used for Consortium MPC), 2 for both publisher and partner XOR encrypted input (used with PS3I)");
DEFINE_bool(
    log_cost,
    false,
    "Log cost info into cloud which will be used for dashboard");
DEFINE_string(log_cost_s3_bucket, "cost-estimation-logs", "s3 bucket name");
DEFINE_string(
    log_cost_s3_region,
    ".s3.us-west-2.amazonaws.com/",
    "s3 region name");
DEFINE_bool(use_new_output_format, false, "New Format of Attribution output");
DEFINE_string(
    run_id,
    "",
    "A run_id used to identify all the logs in a PL/PA run.");
DEFINE_string(
    pc_feature_flags,
    "",
    "A String of PC Feature Flags passing from PCS, separated by comma");
