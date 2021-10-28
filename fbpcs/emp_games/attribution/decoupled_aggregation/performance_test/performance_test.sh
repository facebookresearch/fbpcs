#!/usr/bin/env bash
# Copyright (c) Facebook, Inc. and its affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

cd ~/fbsource/fbcode && buck build //measurement/private_{measurement,lift}/... //identity/private_aggregation/...

for num_rows in 1000 10000 100000 200000
do

cvr=0.5

input_timestamp=$(date +%s)

buck-out/gen/identity/private_aggregation/mpc_aem/data_generator_pa.par private_attribution_data_generator \
--num_pids=${num_rows} \
--max_events_per_pid=4 \
--num_ad_ids=100 \
--batch_start=10000 \
--batch_end=500000 \
--attribution_window=86400 \
--publisher_localpath=./tmp/publisher_test.csv \
--partner_localpath=./tmp/partner_test.csv \
--publisher_s3path="publisher/publisher_${num_rows}_pids_${cvr}_cvr_${input_timestamp}.csv_0" \
--partner_s3path="partner/partner_${num_rows}_pids_${cvr}_cvr_${input_timestamp}.csv_0" \
--conversion_bits_version=3 \
--conversion_rate=${cvr} \
--format_type=all \
--upload_type=S3 \
--s3region=us-west-2 \
--s3bucket=decoupled-attribution \
--s3secret_group=AWS_SVC_592513842793_TERRAFORM

for attribution_rule in "last_click_1d" "last_touch_1d"
do
attribution_timestamp=$(date +%s)
buck run //identity/private_aggregation/mpc_aem:performance_test -- \
--build_target="//fbpcs/emp_games:decoupled_attribution" \
--build_mode="@mode/opt" \
--build_output="temp_mpc_aem_test_binary" \
--aggregators="measurement" \
--attribution_rules="${attribution_rule}" \
--publisher_input_basepath="https://decoupled-attribution.s3.us-west-2.amazonaws.com/publisher/publisher_${num_rows}_pids_${cvr}_cvr_${input_timestamp}.csv_0" \
--publisher_output_basepath="https://decoupled-attribution.s3.us-west-2.amazonaws.com/publisher/publisher_attribution_output_d_${num_rows}_pids_${cvr}_cvr_${attribution_timestamp}_${attribution_rule}.json" \
--partner_input_basepath="https://decoupled-attribution.s3.us-west-2.amazonaws.com/partner/partner_${num_rows}_pids_${cvr}_cvr_${input_timestamp}.csv_0" \
--partner_output_basepath="https://decoupled-attribution.s3.us-west-2.amazonaws.com/partner/partner_attribution_output_d_${num_rows}_pids_${cvr}_cvr_${attribution_timestamp}_${attribution_rule}.json" \
--file_start_index=0 \
--num_files=1 \
--run_name="decoupled_attribution_${num_rows}_pids_${cvr}_cvr" \
--s3secret_group="AWS_SVC_592513842793_TERRAFORM" \
--unixname="${USER}" \
--log_to_sandbox_table >> TestLogs_decoupled_attribution.txt

aggregation_timestamp=$(date +%s)
buck run //identity/private_aggregation/mpc_aem:performance_test -- \
--build_target="//fbpcs/emp_games:decoupled_aggregation" \
--build_mode="@mode/opt" \
--build_output="temp_mpc_aem_test_binary" \
--aggregators="measurement" \
--attribution_rules="${attribution_rule}" \
--publisher_input_basepath="https://decoupled-attribution.s3.us-west-2.amazonaws.com/publisher/publisher_${num_rows}_pids_${cvr}_cvr_${input_timestamp}.csv_0" \
--publisher_output_basepath="https://decoupled-attribution.s3.us-west-2.amazonaws.com/publisher/publisher_aggregation_output_d_${num_rows}_pids_${cvr}_cvr_${aggregation_timestamp}_${attribution_rule}.json" \
--partner_input_basepath="https://decoupled-attribution.s3.us-west-2.amazonaws.com/partner/partner_${num_rows}_pids_${cvr}_cvr_${input_timestamp}.csv_0" \
--publisher_input_base_path_secret_share="https://decoupled-attribution.s3.us-west-2.amazonaws.com/publisher/publisher_attribution_output_d_${num_rows}_pids_${cvr}_cvr_${attribution_timestamp}_${attribution_rule}.json" \
--partner_input_base_path_secret_share="https://decoupled-attribution.s3.us-west-2.amazonaws.com/partner/partner_attribution_output_d_${num_rows}_pids_${cvr}_cvr_${attribution_timestamp}_${attribution_rule}.json" \
--partner_output_basepath="https://decoupled-attribution.s3.us-west-2.amazonaws.com/partner/partner_aggregation_output_d_${num_rows}_pids_${cvr}_cvr_${aggregation_timestamp}_${attribution_rule}.json" \
--file_start_index=0 \
--num_files=1 \
--run_name="decoupled_aggregation_${num_rows}_pids_${cvr}_cvr" \
--s3secret_group="AWS_SVC_592513842793_TERRAFORM" \
--unixname="${USER}" \
--use_xor_encryption=false \
--log_to_sandbox_table >> TestLogs_decoupled_aggregation.txt

existing_attribution_timestamp=$(date +%s)
buck run //identity/private_aggregation/mpc_aem:performance_test -- \
--build_target="//fbpcs/emp_games:attribution" \
--build_mode="@mode/opt" \
--build_output="temp_mpc_aem_test_binary" \
--aggregators="measurement" \
--attribution_rules="${attribution_rule}" \
--publisher_input_basepath="https://decoupled-attribution.s3.us-west-2.amazonaws.com/publisher/publisher_${num_rows}_pids_${cvr}_cvr_${input_timestamp}.csv" \
--publisher_output_basepath="https://decoupled-attribution.s3.us-west-2.amazonaws.com/publisher/publisher_attribution_output_e_${num_rows}_pids_${cvr}_cvr_${existing_attribution_timestamp}_${attribution_rule}.json" \
--partner_input_basepath="https://decoupled-attribution.s3.us-west-2.amazonaws.com/partner/partner_${num_rows}_pids_${cvr}_cvr_${input_timestamp}.csv" \
--partner_output_basepath="https://decoupled-attribution.s3.us-west-2.amazonaws.com/partner/partner_attribution_output_e_${num_rows}_pids_${cvr}_cvr_${existing_attribution_timestamp}_${attribution_rule}.json" \
--file_start_index=0 \
--num_files=1 \
--run_name="existing_attribution_cost_${num_rows}_pids_${cvr}_cvr" \
--s3secret_group="AWS_SVC_592513842793_TERRAFORM" \
--unixname="${USER}" \
--use_xor_encryption=false \
--log_to_sandbox_table >> TestLogs_existing.txt

buck-out/gen/measurement/private_lift/validate_util.par \
"instance_${num_rows}_pids_${cvr}_cvr_${input_timestamp}_${attribution_rule}" \
"https://decoupled-attribution.s3.us-west-2.amazonaws.com/publisher/publisher_aggregation_output_d_${num_rows}_pids_${cvr}_cvr_${aggregation_timestamp}_${attribution_rule}.json" \
"https://decoupled-attribution.s3.us-west-2.amazonaws.com/publisher/publisher_attribution_output_e_${num_rows}_pids_${cvr}_cvr_${existing_attribution_timestamp}_${attribution_rule}.json_0" \
"us-west-2" \
"AWS_SVC_592513842793_TERRAFORM" >> TestLogs_results.txt

done
done
