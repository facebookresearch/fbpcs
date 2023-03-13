/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "fbpcs/emp_games/he_aggregation/HEAggOptions.h"

#include <gflags/gflags.h>

DEFINE_int32(party, 1, "1 = publisher, 2 = partner");
DEFINE_string(server_ip, "127.0.0.1", "Server's IP address");
DEFINE_int32(port, 5000, "Server's port");
DEFINE_string(
    input_base_path_secret_share,
    "",
    "Local or s3 base path for the secret share attribution results.");
DEFINE_string(input_base_path, "", "Local or s3 base path for the input file");
DEFINE_string(
    output_base_path,
    "",
    "Local or s3 base path where output files are written to");
DEFINE_double(delta, 1e-6, "DP noise parameter (delta)");
DEFINE_double(eps, 5, "DP noise parameter (epsilon)");
DEFINE_string(
    run_name,
    "",
    "A user given run name that will be used in s3 filename");
DEFINE_bool(
    log_cost,
    false,
    "Log cost info into cloud which will be used for dashboard");
DEFINE_bool(
    add_dp_noise,
    true,
    "If true, dp noise will not be added to the output.");
DEFINE_string(log_cost_s3_bucket, "", "s3 bucket name");
DEFINE_string(
    log_cost_s3_region,
    ".s3.us-west-2.amazonaws.com/",
    "s3 region name");
DEFINE_bool(
    use_tls,
    false,
    "Whether to use TLS when communicating with other parties.");
DEFINE_string(
    ca_cert_path,
    "",
    "Relative file path where root CA cert is stored. It will be prefixed with $HOME.");
DEFINE_string(
    server_cert_path,
    "",
    "Relative file path where server cert is stored. It will be prefixed with $HOME.");
DEFINE_string(
    private_key_path,
    "",
    "Relative file path where private key is stored. It will be prefixed with $HOME.");
DEFINE_int32(
    input_encryption,
    0,
    "0 for plaintext input, 1 for partner XOR encrypted input (used for Consortium MPC), 2 for both publisher and partner XOR encrypted input (used with PS3I)");
