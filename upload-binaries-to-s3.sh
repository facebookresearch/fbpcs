#!/bin/bash
# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

set -e

PROG_NAME=$0
usage() {
  cat << EOF >&2
Usage: $PROG_NAME <emp_games|data_processing|pid> <tag>

package:
  emp_games - extracts the binaries from fbpcs/emp-games docker image
  data_processing - extracts the binaries from fbpcs/data-processing docker image
  pid - extracts the binaries from private-id docker image
  tag: used to determine the subfolder/version in s3 for each binary
EOF
  exit 1
}

PACKAGES="emp_games data_processing pid"
PACKAGE=$1
TAG=$2
if [[ ! " $PACKAGES " =~ $PACKAGE ]] || [[ ! " $TAG " =~ $TAG ]]; then
   usage
fi
shift

one_docker_repo="one-docker-repository-prod"
lift_package="s3://$one_docker_repo/private_lift/lift/${TAG}/lift"
attribution_repo="s3://$one_docker_repo/private_attribution"
decoupled_attribution="$attribution_repo/decoupled_attribution/${TAG}/decoupled_attribution"
decoupled_aggregation="$attribution_repo/decoupled_aggregation/${TAG}/decoupled_aggregation"
pcf2_attribution="s3://$one_docker_repo/pcf2_attribution/${TAG}/pcf2_attribution"
pcf2_aggregation="s3://$one_docker_repo/pcf2_aggregation/${TAG}/pcf2_aggregation"
shard_aggregator_package="$attribution_repo/shard-aggregator/${TAG}/shard-aggregator"
data_processing_repo="s3://$one_docker_repo/data_processing"
private_id_repo="s3://$one_docker_repo/pid"

if [ "$PACKAGE" = "emp_games" ]; then
cd binaries_out || exit
aws s3 cp lift_calculator "$lift_package"
aws s3 cp decoupled_attribution_calculator "$decoupled_attribution"
aws s3 cp decoupled_aggregation_calculator "$decoupled_aggregation"
aws s3 cp pcf2_attribution_calculator "$pcf2_attribution"
aws s3 cp pcf2_aggregation_calculator "$pcf2_aggregation"
aws s3 cp shard_aggregator "$shard_aggregator_package"
cd .. || exit
fi

if [ "$PACKAGE" = "data_processing" ]; then
cd binaries_out || exit
echo "$data_processing_repo/sharder/${TAG}/sharder"
aws s3 cp sharder "$data_processing_repo/sharder/${TAG}/sharder"
aws s3 cp sharder_hashed_for_pid "$data_processing_repo/sharder_hashed_for_pid/${TAG}/sharder_hashed_for_pid"
aws s3 cp pid_preparer "$data_processing_repo/pid_preparer/${TAG}/pid_preparer"
aws s3 cp lift_id_combiner "$data_processing_repo/lift_id_combiner/${TAG}/lift_id_combiner"
aws s3 cp attribution_id_combiner "$data_processing_repo/attribution_id_combiner/${TAG}/attribution_id_combiner"
fi

if [ "$PACKAGE" = "pid" ]; then
cd binaries_out || exit
aws s3 cp private-id-server "$private_id_repo/private-id-server/${TAG}/private-id-server"
aws s3 cp private-id-client "$private_id_repo/private-id-client/${TAG}/private-id-client"
aws s3 cp private-id-multi-key-server "$private_id_repo/private-id-multi-key-server/${TAG}/private-id-multi-key-server"
aws s3 cp private-id-multi-key-client "$private_id_repo/private-id-multi-key-client/${TAG}/private-id-multi-key-client"
fi
