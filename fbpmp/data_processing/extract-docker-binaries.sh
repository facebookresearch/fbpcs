#!/bin/bash
# Copyright (c) Facebook, Inc. and its affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

set -e
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
    # Run from the root dir of data_processing so the binaries paths exist
    cd "$SCRIPT_DIR" || exit
    mkdir -p binaries_out

docker create -ti --name data_processing_copy "data_processing:${TAG}"
docker cp data_processing_copy:/usr/local/bin/sharder "$SCRIPT_DIR/binaries_out/."
docker cp data_processing_copy:/usr/local/bin/sharder_hashed_for_pid "$SCRIPT_DIR/binaries_out/."
docker cp data_processing_copy:/usr/local/bin/pid_preparer "$SCRIPT_DIR/binaries_out/."
docker cp data_processing_copy:/usr/local/bin/lift_id_combiner "$SCRIPT_DIR/binaries_out/."
docker cp data_processing_copy:/usr/local/bin/attribution_id_combiner "$SCRIPT_DIR/binaries_out/."
docker rm -f data_processing_copy
