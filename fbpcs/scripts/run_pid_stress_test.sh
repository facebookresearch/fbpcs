#!/bin/bash
# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

protocol="$1"
intersect_size=20
sk_partner_sample_size=5000000
mk_partner_sample_size=5000000
mk_publisher_sample_size=5000000
token_size=64

run_and_save_test() {
    product=$1
    partner_sample_size=$2
    publisher_sample_size=$3
    partner_num_ids_start=$4
    partner_num_ids_end=$5
    publisher_num_ids_start=$6
    publisher_num_ids_end=$7
    for (( publisher_num_ids=publisher_num_ids_start; publisher_num_ids<=publisher_num_ids_end; publisher_num_ids++ ))
    do
      for (( partner_num_ids=partner_num_ids_start; partner_num_ids<=partner_num_ids_end; partner_num_ids++ ))
      do
        echo command: buck run //identity/private_aggregation/mpc_aem/experimentation_platform:mpc_aem_exp_platform_runner -- "benchmark_${product}_${intersect_size}_${partner_sample_size}_${publisher_sample_size}_${token_size}_${partner_num_ids}_${publisher_num_ids}" \
        --publisher_input="https://mpc-aem-exp-platform-input.s3.us-west-2.amazonaws.com/pid_test_data/stress_test/test_${product}_server_${intersect_size}_${partner_sample_size}_${publisher_sample_size}_${token_size}_${partner_num_ids}_${publisher_num_ids}.csv" \
        --partner_input="https://mpc-aem-exp-platform-input.s3.us-west-2.amazonaws.com/pid_test_data/stress_test/test_${product}_client_${intersect_size}_${partner_sample_size}_${publisher_sample_size}_${token_size}_${partner_num_ids}_${publisher_num_ids}.csv" \
        --game="${product}" --tier="test"

        buck run //identity/private_aggregation/mpc_aem/experimentation_platform:mpc_aem_exp_platform_runner -- "benchmark_${product}_${intersect_size}_${partner_sample_size}_${publisher_sample_size}_${token_size}_${partner_num_ids}_${publisher_num_ids}" \
        --publisher_input="https://mpc-aem-exp-platform-input.s3.us-west-2.amazonaws.com/pid_test_data/stress_test/test_${product}_server_${intersect_size}_${partner_sample_size}_${publisher_sample_size}_${token_size}_${partner_num_ids}_${publisher_num_ids}.csv" \
        --partner_input="https://mpc-aem-exp-platform-input.s3.us-west-2.amazonaws.com/pid_test_data/stress_test/test_${product}_client_${intersect_size}_${partner_sample_size}_${publisher_sample_size}_${token_size}_${partner_num_ids}_${publisher_num_ids}.csv" \
        --game="${product}" --tier="test" 2>&1 | tee "$HOME/logs/${product}_${intersect_size}_${partner_sample_size}_${publisher_sample_size}_${token_size}_${partner_num_ids}_${publisher_num_ids}.log" || true

        pastry -t "${product}_${intersect_size}_${partner_sample_size}_${publisher_sample_size}_${token_size}_${partner_num_ids}_${publisher_num_ids}_python_log" \
        < "$HOME/logs/${product}_${intersect_size}_${partner_sample_size}_${publisher_sample_size}_${token_size}_${partner_num_ids}_${publisher_num_ids}.log"

        for party in "publisher" "partner"
        do
          pastry -t "${party}_benchmark_${product}_${intersect_size}_${partner_sample_size}_${publisher_sample_size}_${token_size}_${partner_num_ids}_${publisher_num_ids}" \
          < "./${party}_benchmark_${product}_${intersect_size}_${partner_sample_size}_${publisher_sample_size}_${token_size}_${partner_num_ids}_${publisher_num_ids}0"
        done
      done
    done

}

for product in "lift" "attribution"
do
  if [ "$protocol" = "single-key" ]
  then
    for sk_publisher_sample_size in 5000000 10000000
    do
      run_and_save_test $product $sk_partner_sample_size $sk_publisher_sample_size 1 1 1 1
    done
  fi

  if [ "$protocol" = "multi-key" ]
  then
    run_and_save_test $product $mk_partner_sample_size $mk_publisher_sample_size 1 5 1 12
  fi
done
