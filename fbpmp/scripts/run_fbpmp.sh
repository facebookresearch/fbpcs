#!/usr/bin/env bash
# Copyright (c) Facebook, Inc. and its affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

set -e

REAL_INSTANCE_REPO=$(realpath 'fbpmp_instances')
mkdir -p "$REAL_INSTANCE_REPO"

REAL_CREDENTIALS_PATH="${HOME}/.aws/credentials"

DOCKER_INSTANCE_REPO='/fbpmp_instances'
DOCKER_CONFIG_PATH="/config.yml"
DOCKER_CREDENTIALS_PATH="/root/.aws/credentials"

ECR_URL='539290649537.dkr.ecr.us-west-2.amazonaws.com'
IMAGE_NAME='pl-coordinator-env'
TAG='latest'
DOCKER_IMAGE="${ECR_URL}/${IMAGE_NAME}:${TAG}"

DEPENDENCIES=( docker aws )

function usage ()
{
    echo "Usage :  $0 <lift|attribution> <rest of command>"
    echo "e.g. $0 lift create_instance 123 --config='path/to/config.yml' --role=partner"
}

function main () {
    check_dependencies
    parse_args "$@"
    run_fbpmp
}

function check_dependencies() {
    for cmd in "${DEPENDENCIES[@]}"
    do
        command -v "$cmd" >/dev/null 2>&1 || { echo "Could not find ${cmd} - is it installed?" >&2; exit 1; }
    done
}

function parse_args() {
    if [[ $# -eq 0 ]]; then
        usage
        exit 1
    fi

    docker_cmd=( python3.8 -m )

    case $1 in
        lift )
            docker_cmd+=('fbpmp.pl_coordinator.pl_coordinator')
            ;;
        attribution )
            docker_cmd+=('fbpmp.pa_coordinator.pa_coordinator')
            ;;
        * )
            >&2 echo "$1 is not a valid game."
            usage
            exit 1
            ;;
    esac
    shift

    for arg in "$@"
    do
        case $arg in
            --config=*)
                real_config_path=$(realpath "${arg#*=}")
                if [[ ! -f "$real_config_path" ]]; then
                    >&2 echo "$real_config_path does not exist."
                    usage
                    exit 1
                fi
                docker_cmd+=("--config=${DOCKER_CONFIG_PATH}")
                ;;
            *)
                docker_cmd+=("$arg")
                ;;
        esac
        shift
    done

    if [ -z ${real_config_path+x} ]; then
        >&2 echo "--config=<path to config> not specified in coordinator command."
        usage
        exit 1
    fi
}

function run_fbpmp() {
    aws ecr get-login-password --region us-west-2 | docker login --username AWS --password-stdin "$ECR_URL"
    docker pull "$DOCKER_IMAGE"
    docker run --rm \
        -v "$real_config_path":"$DOCKER_CONFIG_PATH" \
        -v "$REAL_INSTANCE_REPO":"$DOCKER_INSTANCE_REPO" \
        -v "$REAL_CREDENTIALS_PATH":"$DOCKER_CREDENTIALS_PATH" \
        ${DOCKER_IMAGE} "${docker_cmd[@]}"
}

main "$@"
