#!/usr/bin/env bash
# Copyright (c) Facebook, Inc. and its affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

set -e

REAL_INSTANCE_REPO=$(realpath 'fbpcs_instances')
mkdir -p "$REAL_INSTANCE_REPO"

REAL_CREDENTIALS_PATH="${HOME}/.aws/credentials"

DOCKER_INSTANCE_REPO='/fbpcs_instances'
DOCKER_CONFIG_PATH="/config.yml"
DOCKER_CREDENTIALS_PATH="/root/.aws/credentials"

ECR_URL='539290649537.dkr.ecr.us-west-2.amazonaws.com'
IMAGE_NAME='pl-coordinator-env'
TAG='latest'
DOCKER_IMAGE="${ECR_URL}/${IMAGE_NAME}:${TAG}"

DEPENDENCIES=( docker aws )

function usage ()
{
    echo "Usage :  $0 <command with arguments>"
    echo "e.g. $0 get_instance 123 --config='path/to/config.yml'"
}

function main () {
    check_dependencies
    parse_args "$@"
    run_fbpcs
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

    docker_cmd=( python3.8 -m fbpcs.private_computation_cli.private_computation_cli )

    RED='\033[0;31m'
    NC='\033[0m' # No Color

    case $1 in
        lift )
            echo -e "${RED}********************************************************"
            echo -e "Because you specified game name \"$1\", it looks like you're running a deprecated version of this command."
            echo -e "Please remove \"$1\" from the command and try again."
            echo -e "********************************************************${NC}"
            exit 1
            ;;
        attribution )
            echo -e "${RED}********************************************************"
            echo -e "Because you specified game name \"$1\", it looks like you're running a deprecated version of this command."
            echo -e "Please modify your commands according to the instruction below and try again:"
            echo -e "1. Remove \"attribution\" right after \"run_fbpcs\""
            echo -e "2. Add a new argument \"--game_type=attribution\" to the \"run_fbpcs.sh create_instance ...\" command"
            echo -e "3. Replace \"compute_attribution\" with \"compute_metrics\""
            echo -e "********************************************************${NC}"
            exit 1
            ;;
    esac

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

function run_fbpcs() {
    aws ecr get-login-password --region us-west-2 | docker login --username AWS --password-stdin "$ECR_URL"
    docker pull "$DOCKER_IMAGE"
    docker run --rm \
        -v "$real_config_path":"$DOCKER_CONFIG_PATH" \
        -v "$REAL_INSTANCE_REPO":"$DOCKER_INSTANCE_REPO" \
        -v "$REAL_CREDENTIALS_PATH":"$DOCKER_CREDENTIALS_PATH" \
        ${DOCKER_IMAGE} "${docker_cmd[@]}"
}

main "$@"
