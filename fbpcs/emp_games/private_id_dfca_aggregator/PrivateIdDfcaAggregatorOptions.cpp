/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <gflags/gflags.h>

#include "fbpcs/emp_games/private_id_dfca_aggregator/PrivateIdDfcaAggregatorOptions.h"

DEFINE_int32(party, 1, "1 = publisher, 2 = partner");
DEFINE_string(server_ip, "", "Server's IP address");
DEFINE_int32(port, 15200, "Server's port");
DEFINE_string(input_base_path, "", "Input path where input files are located");
DEFINE_int32(
    first_shard_index,
    0,
    "index of first shard in input_path, first filename input_path_[first_shard_index]");
DEFINE_int32(
    num_shards,
    1,
    "Number of shards from input_path_[0] to input_path_[n-1]");
DEFINE_string(output_path, "", "Output path where output file is located");
DEFINE_string(run_name, "", "User given name used to write cost info in S3");
DEFINE_bool(
    log_cost,
    false,
    "Log cost info into cloud which will be used for dashboard");
DEFINE_string(log_cost_s3_bucket, "cost-estimation-logs", "s3 bucket name");
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
