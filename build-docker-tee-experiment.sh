#!/bin/bash
# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

set -e

UBUNTU_RELEASE="20.04"
GITHUB_PACKAGES="ghcr.io/facebookresearch"


PROG_NAME=$0
usage() {
  cat << EOF >&2
Usage: $PROG_NAME [-u] [-g] [-t TAG]

-u: builds the docker images against ubuntu (default)
-f: force use of latest fbpcf from ghcr.io/facebookresearch
-g Only used for the pce_deployment package to build the GCP docker image instead of the AWS image
-t TAG: tags the image with the given tag (default: latest)
EOF
  exit 1
}

IMAGE_PREFIX=""
OS_VARIANT="ubuntu"
OS_RELEASE=${UBUNTU_RELEASE}
DOCKER_EXTENSION=".ubuntu"
TAG="latest"
FORCE_EXTERNAL=false
USE_GCP=false
while getopts "u,f,g,t:" o; do
  case $o in
    (u) OS_VARIANT="ubuntu"
        OS_RELEASE=${UBUNTU_RELEASE}
        DOCKER_EXTENSION=".ubuntu";;
    (f) FORCE_EXTERNAL=true;;
    (g) USE_GCP=true;;
    (t) TAG=$OPTARG;;
    (*) usage
  esac
done
shift "$((OPTIND - 1))"

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
# Docker build must run from the root folder, so all the relative paths work inside the Dockerfile's
cd "$SCRIPT_DIR" || exit

FBPCF_IMAGE="fbpcf/${OS_VARIANT}:latest"
IMAGE_PREFIX="fbpcs/" # Current all FBPCF Dependent images are tagged with "fbpcs" prefix
if [ "${FORCE_EXTERNAL}" == false ] && docker image inspect "${FBPCF_IMAGE}" >/dev/null 2>&1; then
  printf "Using locally built %s docker image (this may NOT be up-to-date...)\n\n" "${FBPCF_IMAGE}"
  printf "To use latest %s from %s please \n   1. Delete this local docker image (docker image rm %s) \nor\n   2. Use the '-f' flag\n" "${FBPCF_IMAGE}" "${GITHUB_PACKAGES}" "${FBPCF_IMAGE}"
else
  printf "attempting to pull image %s from %s...\n" "${FBPCF_IMAGE}" "${GITHUB_PACKAGES}"
  if docker pull "${GITHUB_PACKAGES}/${FBPCF_IMAGE}" 2>/dev/null; then
      # Use the external ghcr.io image (if a local image doesn't exist) instead of building locally...
      FBPCF_IMAGE="${GITHUB_PACKAGES}/${FBPCF_IMAGE}"
      printf "successfully pulled image %s\n\n" "${FBPCF_IMAGE}"
    else
      # This should not happen since we build fbpcf externally
      printf "\nERROR: Unable to find docker image %s.  Please clone and run https://github.com/facebookresearch/fbpcf/blob/master/build-docker.sh " "${FBPCF_IMAGE}"
      exit 1
  fi
fi

DOCKER_PACKAGE="tee-experiment"
printf "\nBuilding %s %s docker image...\n" "${DOCKER_PACKAGE}" "${OS_VARIANT}"
docker build  \
  --build-arg tag="${TAG}" \
  --build-arg os_release="${OS_RELEASE}" \
  --build-arg fbpcf_image="${FBPCF_IMAGE}" \
  --compress \
  -t "${IMAGE_PREFIX}${DOCKER_PACKAGE}:${TAG}" -f "docker/tee_experiment/Dockerfile${DOCKER_EXTENSION}" .
