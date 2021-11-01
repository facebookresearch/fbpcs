#!/bin/bash
# Copyright (c) Facebook, Inc. and its affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

# Run this script at the root folder with ./src/scripts/private_attribution/partner/id_match_stage_partner.sh

# ------- configurable parameters -------

config="./config.yml"
s3_path_prefix="https://<s3_bucket>.s3.us-west-2.amazonaws.com/attribution/${USER}/E2E"
partner_instance="123_partner"

# id match parameters
id_match_input_path="${s3_path_prefix}/id_match_stage/partner_input.csv"
id_match_output_path="${s3_path_prefix}/id_match_stage/partner_output.csv"
server_ips=10.0.0.1 # ask Publisher
num_pid_containers=1 # ask Publisher for num_shards generated from Publsiher side

# data processing parameters
num_mpc_containers=1 # ask Publisher
num_files_per_mpc_container=1 # ask Publisher
hmac_key="" # ask Publisher

# ------- create partner instance -------

rm ${partner_instance} ${partner_instance}_id_match 2> /dev/null

echo "Create a partner instance..."

python3.8 -m fbpcs.private_computation_cli.private_computation_cli \
  create_instance ${partner_instance} \
  --config="${config}" \
  --role=partner

# ------- run id match -------------

echo "Start id match stage..."

python3.8 -m fbpcs.private_computation_cli.private_computation_cli \
  id_match ${partner_instance} \
  --config="${config}" \
  --num_shards=${num_pid_containers} \
  --input_path="${id_match_input_path}" \
  --output_path="${id_match_output_path}" \
  --server_ips="${server_ips}" \
  --hmac_key="${hmac_key}" \
  --dry_run

echo "Finish id match stage..."

# ------- run data processing --------

echo "Start data processing stage..."

data_processing_data_path="${id_match_output_path}_advertiser_sharded"
data_processing_spine_path="${id_match_output_path}_advertiser_pid_matched"
data_processing_output_path="${s3_path_prefix}/prepare_data_stage/partner_output.csv"

python3.8 -m fbpcs.private_computation_cli.private_computation_cli \
  prepare_compute_input ${partner_instance} \
  --config="${config}" \
  --num_pid_containers=${num_pid_containers}  \
  --num_mpc_containers=${num_mpc_containers}  \
  --num_files_per_mpc_container=${num_files_per_mpc_container} \
  --data_path="${data_processing_data_path}" \
  --spine_path="${data_processing_spine_path}" \
  --output_path="${data_processing_output_path}" \
  --dry_run
