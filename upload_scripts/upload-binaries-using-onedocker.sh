#!/bin/bash
# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

set -e

PROG_NAME=$0
usage() {
  cat << EOF >&2
Usage: $PROG_NAME <emp_games|data_processing|pid|validation|smart_agent> <tag> -c <config>

package:
  emp_games - extracts the binaries from fbpcs/emp-games docker image
  data_processing - extracts the binaries from fbpcs/data-processing docker image
  pid - extracts the binaries from private-id docker image
  validation - extracts the binaries from the onedocker docker image
  smart_agent - extracts the binaries from the onedocker docker image

tag: used to determine the subfolder/version in s3 for each binary

-c A path to the YAML configuration for the OneDocker Repository Service from the repo root. It defaults to 'upload_scripts/configs/test_upload_binaries_config.yml'
EOF
  exit 1
}

PACKAGES="emp_games data_processing pid validation smart_agent"
PACKAGE=$1
TAG=$2

if [[ ! " $PACKAGES " =~ $PACKAGE ]] || [[ ! " $TAG " =~ $TAG ]]; then
   usage
fi
shift 2

ONEDOCKER_CONFIG="../upload_scripts/configs/test_upload_binaries_config.yml"
while getopts 'c:' OPTION; do
  case "$OPTION" in
    c)
      ONEDOCKER_CONFIG="../$OPTARG"
      ;;
    ?)
      usage
      ;;
  esac
done
shift "$((OPTIND -1))"

onedocker_upload() {
  package="$1"
  package_path="$2"
  python3 -m onedocker.script.cli upload --config="$ONEDOCKER_CONFIG" --package_name="$package" --package_path="$package_path" --version="$TAG"
}

lift_path="private_lift/lift"
pcf2_lift_path="private_lift/pcf2_lift"
pcf2_lift_metadata_compaction_path="private_lift/pcf2_lift_metadata_compaction"
decoupled_attribution_path="private_attribution/decoupled_attribution"
decoupled_aggregation_path="private_attribution/decoupled_aggregation"
pcf2_attribution_path="private_attribution/pcf2_attribution"
pcf2_aggregation_path="private_attribution/pcf2_aggregation"
shard_aggregator_path="private_attribution/shard-aggregator"
pcf2_shard_combiner_path="private_attribution/pcf2_shard-combiner"
private_id_dfca_aggregator_path="private_id_dfca/private_id_dfca_aggregator"

if [ "$PACKAGE" = "emp_games" ]; then
cd binaries_out || exit
onedocker_upload "$lift_path" lift_calculator
onedocker_upload "$pcf2_lift_path" pcf2_lift_calculator
onedocker_upload "$pcf2_lift_metadata_compaction_path" pcf2_lift_metadata_compaction
onedocker_upload "$decoupled_attribution_path" decoupled_attribution_calculator
onedocker_upload "$decoupled_aggregation_path" decoupled_aggregation_calculator
onedocker_upload "$pcf2_attribution_path" pcf2_attribution_calculator
onedocker_upload "$pcf2_aggregation_path" pcf2_aggregation_calculator
onedocker_upload "$shard_aggregator_path" shard_aggregator
onedocker_upload "$pcf2_shard_combiner_path" pcf2_shard_combiner
onedocker_upload "$private_id_dfca_aggregator_path" private_id_dfca_aggregator
cd .. || exit
fi

if [ "$PACKAGE" = "data_processing" ]; then
cd binaries_out || exit
onedocker_upload "data_processing/sharder" sharder
onedocker_upload "data_processing/sharder_hashed_for_pid" sharder_hashed_for_pid
onedocker_upload "data_processing/secure_random_sharder" secure_random_sharder
onedocker_upload "data_processing/pid_preparer" pid_preparer
onedocker_upload "data_processing/lift_id_combiner" lift_id_combiner
onedocker_upload "data_processing/attribution_id_combiner" attribution_id_combiner
onedocker_upload "data_processing/private_id_dfca_id_combiner" private_id_dfca_id_combiner
cd .. || exit
fi

if [ "$PACKAGE" = "pid" ]; then
cd binaries_out || exit
onedocker_upload pid/private-id-server private-id-server
onedocker_upload pid/private-id-client private-id-client
onedocker_upload pid/private-id-multi-key-server private-id-multi-key-server
onedocker_upload pid/private-id-multi-key-client private-id-multi-key-client
cd .. || exit
fi

if [ "$PACKAGE" = "validation" ]; then
cd binaries_out || exit
onedocker_upload validation/pc_pre_validation_cli pc_pre_validation_cli
cd .. || exit
fi

if [ "$PACKAGE" = "smart_agent" ]; then
cd binaries_out || exit
onedocker_upload smart_agent/smart_agent_server smart_agent_server
cd .. || exit
fi
