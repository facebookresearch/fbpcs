#!/bin/bash
# Copyright (c) Facebook, Inc. and its affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

PROG_NAME=$0
usage() {
  cat << EOF >&2
Usage: $PROG_NAME [-t TAG]

-t TAG: tags the image with the given tag (default: latest)
EOF
  exit 1
}

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

docker create -ti --name emp_game_copy "emp_game:${TAG}"
docker cp emp_game_copy:/usr/local/bin/lift_calculator "$SCRIPT_DIR/binaries_out/."
docker cp emp_game_copy:/usr/local/bin/attribution_calculator "$SCRIPT_DIR/binaries_out/."
docker cp emp_game_copy:/usr/local/bin/shard_aggregator "$SCRIPT_DIR/binaries_out/."
docker rm -f emp_game_copy
