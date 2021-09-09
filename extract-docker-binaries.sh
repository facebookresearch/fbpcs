#!/bin/bash
# Copyright (c) Facebook, Inc. and its affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

set -e

PROG_NAME=$0
usage() {
  cat << EOF >&2
Usage: $PROG_NAME <emp_games|data_processing|fbpcs> [-t TAG]

package:
  emp_games - builds the emp_games docker image
  data_processing - builds the data_processing docker image
-t TAG: uses the image with the given tag (default: latest)
EOF
  exit 1
}

PACKAGES="emp_games data_processing"
PACKAGE=$1
if [[ ! " $PACKAGES " =~ $PACKAGE ]]; then
   usage
fi
shift

TAG="latest"
while getopts "t:" o; do
  case $o in
    (t) TAG=$OPTARG;;
    (*) usage
  esac
done
shift "$((OPTIND - 1))"


SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
    # Run from the root dir of emp_games so the binaries paths exist
    cd "$SCRIPT_DIR" || exit
    mkdir -p binaries_out

if [ "$PACKAGE" = "emp_games" ]; then
docker create -ti --name temp_container "emp-games:${TAG}"
docker cp temp_container:/usr/local/bin/lift_calculator "$SCRIPT_DIR/binaries_out/."
docker cp temp_container:/usr/local/bin/attribution_calculator "$SCRIPT_DIR/binaries_out/."
docker cp temp_container:/usr/local/bin/shard_aggregator "$SCRIPT_DIR/binaries_out/."
docker rm -f temp_container
fi

if [ "$PACKAGE" = "data_processing" ]; then
docker create -ti --name temp_container "data-processing:${TAG}"
docker cp temp_container:/usr/local/bin/sharder "$SCRIPT_DIR/binaries_out/."
docker cp temp_container:/usr/local/bin/sharder_hashed_for_pid "$SCRIPT_DIR/binaries_out/."
docker cp temp_container:/usr/local/bin/pid_preparer "$SCRIPT_DIR/binaries_out/."
docker cp temp_container:/usr/local/bin/lift_id_combiner "$SCRIPT_DIR/binaries_out/."
docker cp temp_container:/usr/local/bin/attribution_id_combiner "$SCRIPT_DIR/binaries_out/."
docker rm -f temp_container
fi
