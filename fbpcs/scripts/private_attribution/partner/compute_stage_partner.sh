#!/bin/bash
# Copyright (c) Facebook, Inc. and its affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

# ------- configurable parameters -------

config="./config.yml"
s3_path_prefix="https://<s3_bucket>.s3.us-west-2.amazonaws.com/attribution/${USER}/E2E"
compute_input_path="${s3_path_prefix}/prepare_data_stage/partner_output.csv"
compute_output_path="${s3_path_prefix}/compute_stage/partner_output.csv"
attribution_rule=last_touch_1d # last_touch_1d or last_click_1d
server_ips=10.0.0.1 # ask Publisher
num_mpc_containers=1 # ask Publisher
num_files_per_mpc_container=1 # ask Publisher
concurrency=4 # ask Publisher
partner_instance="123_partner"

# ------- create instance if it doesn't exist -----------

if [ ! -f ./${partner_instance}  ]; then
  echo "Create a partner instance..."

  python3.8 -m fbpcs.pa_coordinator.pa_coordinator \
    create_instance ${partner_instance}  \
    --config="${config}" \
    --role=partner
fi

# ------- run compute attribution -------

echo "Start compute attribution stage..."

python3.8 -m fbpcs.pa_coordinator.pa_coordinator \
  compute_attribution ${partner_instance} \
  --config="${config}" \
  --num_containers=${num_mpc_containers} \
  --num_files_per_mpc_container=${num_files_per_mpc_container} \
  --attribution_rule=${attribution_rule} \
  --aggregation_type=measurement \
  --concurrency=${concurrency} \
  --input_base_path="${compute_input_path}" \
  --output_base_path="${compute_output_path}" \
  --server_ips="${server_ips}" \
  --dry_run
