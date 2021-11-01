#!/bin/bash
# Copyright (c) Facebook, Inc. and its affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

# ------- configurable parameters -------

config="./config.yml"
s3_path_prefix="https://<s3_bucket>.s3.us-west-2.amazonaws.com/attribution/${USER}/E2E"
partner_input_path="${s3_path_prefix}/compute_stage/partner_output.csv"
partner_output_path="${s3_path_prefix}/aggregate_shards_stage/partner_output.json"
num_mpc_containers_previously_used=1 # Must match with num_mpc_containers in the previous compute stage
num_files_per_mpc_container=1 # ask Publisher
server_ips=10.20.1.1 # Ask Publisher
kanonymity_threshold=100 # Ask Publisher
partner_instance="123_partner"

# ------- create instance if it doesn't exist -----------

if [ ! -f ./"${partner_instance}" ]; then
  echo "Create a partner instance..."

  python3.8 -m fbpcs.private_computation_cli.private_computation_cli \
    create_instance ${partner_instance} \
    --config="${config}" \
    --role=partner
fi

# ------- run aggregate shards -----------

echo "Start aggregate shards stage..."

python3.8 -m fbpcs.private_computation_cli.private_computation_cli \
  aggregate_shards ${partner_instance} \
  --config="${config}" \
  --num_containers_previously_used=${num_mpc_containers_previously_used} \
  --num_files_per_mpc_container=${num_files_per_mpc_container} \
  --threshold=${kanonymity_threshold} \
  --input_base_path="${partner_input_path}" \
  --output_path="${partner_output_path}" \
  --server_ips="${server_ips}" \
  --dry_run
