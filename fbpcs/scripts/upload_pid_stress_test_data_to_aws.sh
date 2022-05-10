#!/bin/bash
# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

account_id=$1

eval $(awsume api "$account_id" SSOAdmin)
export https_proxy=fwdproxy:8080

intersect=20
sk_partner_sample_size=5000000
mk_partner_sample_size=5000000
mk_publisher_sample_size=5000000
token_size=64

upload_all() {
    partner_sample_size=$1
    publisher_sample_size=$2
    publisher_num_ids_start=$3
    publisher_num_ids_end=$4
    partner_num_ids_start=$5
    partner_num_ids_end=$6
    for product in "lift" "attribution"
    do
        for (( publisher_num_ids=publisher_num_ids_start; publisher_num_ids<=publisher_num_ids_end; publisher_num_ids++ ))
        do
            for (( partner_num_ids=partner_num_ids_start; partner_num_ids<=partner_num_ids_end; partner_num_ids++ ))
            do
                aws s3 cp "./pid_stress_test_data/test_${product}_client_${intersect}_${partner_sample_size}_${publisher_sample_size}_${token_size}_${partner_num_ids}_${publisher_num_ids}.csv" "s3://mpc-aem-exp-platform-input/pid_test_data/stress_test/test_${product}_client_${intersect}_${partner_sample_size}_${publisher_sample_size}_${token_size}_${partner_num_ids}_${publisher_num_ids}.csv"
                aws s3 cp "./pid_stress_test_data/test_${product}_server_${intersect}_${partner_sample_size}_${publisher_sample_size}_${token_size}_${partner_num_ids}_${publisher_num_ids}.csv" "s3://mpc-aem-exp-platform-input/pid_test_data/stress_test/test_${product}_server_${intersect}_${partner_sample_size}_${publisher_sample_size}_${token_size}_${partner_num_ids}_${publisher_num_ids}.csv"
            done
        done
    done
}

for sk_publisher_sample_size in 5000000 10000000
do
    upload_all $sk_partner_sample_size $sk_publisher_sample_size 1 1 1 1
done

upload_all $mk_partner_sample_size $mk_publisher_sample_size 1 12 1 5
