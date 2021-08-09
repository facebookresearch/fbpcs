#!/bin/bash
# Copyright (c) Facebook, Inc. and its affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

set -e

UBUNTU_RELEASE="20.04"

PROG_NAME=$0
usage() {
  cat << EOF >&2
Usage: $PROG_NAME [-u] [-t TAG]

-u: builds the docker images against ubuntu (default)
-t TAG: Use the specified tag for the built image (default: latest)
EOF
  exit 1
}

IMAGE_PREFIX="ubuntu"
OS_RELEASE=${UBUNTU_RELEASE}
DOCKER_EXTENSION=".ubuntu"
TAG="latest"
while getopts "u,t:" o; do
  case $o in
    (u) IMAGE_PREFIX="ubuntu"
        OS_RELEASE=${UBUNTU_RELEASE}
        DOCKER_EXTENSION=".ubuntu";;
    (t) TAG=$OPTARG;;
    (*) usage
  esac
done
shift "$((OPTIND - 1))"

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
# Docker build must run from the data_processing folder, so all the relative paths work inside the Dockerfile's
cd "$SCRIPT_DIR" || exit

FBPCF_IMAGE=fbpcf/${IMAGE_PREFIX}:${TAG}
if docker image inspect "${FBPCF_IMAGE}" > /dev/null 2>&1; then
  printf "Found %s docker image..." "${FBPCF_IMAGE}"
else
  # OSS TODO: Update message to reflect github repo NOT internal fbcode
  printf "\nERROR: Unable to find docker image %s.  Please run .../fbsource/fbcode/measurement/private_measurement/pcf/oss/docker-build.sh " "${FBPCF_IMAGE}"
  exit 1
fi

printf "\nBuilding data_processing %s docker image...\n" "${IMAGE_PREFIX}"
docker build  \
    --build-arg os_release=${OS_RELEASE} \
    --compress \
    -t "data_processing:${TAG}" -f docker/Dockerfile${DOCKER_EXTENSION} .
