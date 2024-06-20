/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "fbpcs/emp_games/lift/common/CommonLiftOptions.h"
#include <gflags/gflags.h>

DEFINE_bool(
    compute_publisher_breakdowns,
    true,
    "To enable or disable computing publisher breakdown for result validation");
DEFINE_bool(
    log_cost,
    false,
    "Log cost info into cloud which will be used for dashboard");
DEFINE_bool(
    use_tls,
    false,
    "Whether to use TLS when communicating with other parties.");
DEFINE_bool(
    use_xor_encryption,
    true,
    "Reveal output with XOR secret shares instead of in the clear to both parties");
DEFINE_uint32(concurrency, 1, "max number of games that will run concurrently");
DEFINE_uint32(
    file_start_index,
    0,
    "First file that will be read with base path");
DEFINE_uint32(
    num_conversions_per_user,
    4,
    "Cap and pad to this many conversions per user");
DEFINE_uint32(num_files, 0, "Number of files that should be read");
DEFINE_uint32(party, 1, "1 = publisher, 2 = partner");
DEFINE_uint32(
    port,
    10000,
    "Network port for establishing connection to other player");
DEFINE_uint64(
    epoch,
    1546300800,
    "Unixtime of 2019-01-01. Used as our 'new epoch' for timestamps");
DEFINE_string(
    ca_cert_path,
    "",
    "Relative file path where root CA cert is stored. It will be prefixed with $HOME.");
DEFINE_string(
    input_base_path,
    "",
    "Local or s3 base path for the sharded input files");
DEFINE_string(log_cost_s3_bucket, "", "s3 bucket name");
DEFINE_string(
    log_cost_s3_region,
    ".s3.us-west-2.amazonaws.com/",
    "s3 region name");
DEFINE_string(
    pc_feature_flags,
    "",
    "A String of PC Feature Flags passing from PCS, separated by comma");
DEFINE_string(
    private_key_path,
    "",
    "Relative file path where private key is stored. It will be prefixed with $HOME.");
DEFINE_string(
    run_name,
    "",
    "A user given run name that will be used in s3 filename");
DEFINE_string(
    server_cert_path,
    "",
    "Relative file path where server cert is stored. It will be prefixed with $HOME.");
DEFINE_string(server_ip, "127.0.0.1", "Server's IP Address");
