#!/usr/bin/env bash
# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

set -e

# if these environment variables are not set, use the provided defaults
: "${FBPCS_CONTAINER_REPO_URL:=539290649537.dkr.ecr.us-west-2.amazonaws.com}"
: "${FBPCS_IMAGE_NAME:=pl-coordinator-env}"

REAL_INSTANCE_REPO=$(realpath 'fbpcs_instances')
mkdir -p "$REAL_INSTANCE_REPO"

REAL_CREDENTIALS_PATH="${HOME}/.aws/credentials"

DOCKER_INSTANCE_REPO='/fbpcs_instances'
DOCKER_CONFIG_PATH="/config.yml"
DOCKER_CREDENTIALS_PATH="/root/.aws/credentials"

tag='latest'

DEPENDENCIES=(docker realpath)

function usage() {
  example1="$0 run_study <study_id> --objective_ids=<objective_id_1>,<objective_id_2> \
--config=config.yml --input_paths=https://<s3_conversion_data_file_path_for_objective_1>,\
https://<s3_conversion_data_file_path_for_objective_2> \
--log_path=/fbpcs_instances/output.txt"

  example2="$example1 -- --version=latest"

  echo "Usage :  $0 [PC-CLI options] -- [$0 options]
    PC-CLI Options:
      Refer to handbook for guidance on using PC-CLI

    $0 Options:
      --version=<version>   specify the release version of lift/attribution is used

    Examples:
      $example1

      $example2
    "
}

function main() {
  check_dependencies
  parse_args "$@"
  run_fbpcs || retry_run_fbpcs
}

function check_dependencies() {
  for cmd in "${DEPENDENCIES[@]}"; do
    command -v "$cmd" >/dev/null 2>&1 || {
      echo "Could not find ${cmd} - is it installed?" >&2
      exit 1
    }
  done
}

function replace_config_var() {
  # Creates a new config with the new variable

  # arg 1: var to replace (e.g. binary_version)
  # arg 2: value (e.g. rc)
  # arg 3: config path

  # "to replace" section:
  # capture whitespace, var to replace, and colon into capture group 1
  # match the rest of the line
  # "substitute with" section:
  # keep capture group 1
  # add space back
  # add value (e.g. rc)

  modified_config_path=$(mktemp)
  sed "s/\(\s*${1}:\).*/\1 ${2}/g" "$3" >"$modified_config_path"
  real_config_path="$modified_config_path"
}

function parse_args() {
  if [[ $# -eq 0 ]]; then
    usage
    exit 1
  fi

  docker_cmd=(python3.8 -m fbpcs.private_computation_cli.private_computation_cli)

  # PC-CLI arguments
  for arg in "$@"; do
    case $arg in
      --config=*)
        real_config_path=$(realpath "${arg#*=}")
        if [[ ! -f "$real_config_path" ]]; then
          echo >&2 "$real_config_path does not exist."
          usage
          exit 1
        fi
        docker_cmd+=("--config=${DOCKER_CONFIG_PATH}")
        ;;
      --)
        # separates PC-CLI specific arguments from run_fbpcs.sh specific arguments
        shift
        break
        ;;
      *)
        docker_cmd+=("$arg")
        ;;
    esac
    shift
  done

  if [ -z ${real_config_path+x} ]; then
    # hack to support PC-CLI endpoints that do not require --config
    real_config_path=$(mktemp)
  fi

  # run_fbpcs.sh specific arguments
  for arg in "$@"; do
    case $arg in
      --version=*)
        tag="${arg#*=}"
        echo "Overriding docker version tag and config.yml binary tag with $tag"
        replace_config_var binary_version "$tag" "$real_config_path"
        ;;
      *)
        echo >&2 "$arg is not a valid argument"
        usage
        exit 1
        ;;
    esac
    shift
  done

  docker_image="${FBPCS_CONTAINER_REPO_URL}/${FBPCS_IMAGE_NAME}:${tag}"
}

function run_fbpcs() {
  if [[ "$FBPCS_CONTAINER_REPO_URL" == *"amazonaws"* ]]; then
    aws ecr get-login-password --region us-west-2 | docker login --username AWS --password-stdin "$FBPCS_CONTAINER_REPO_URL"
  fi
  docker pull "$docker_image"
  docker run -e FBPCS_GRAPH_API_TOKEN \
    -v "$real_config_path":"$DOCKER_CONFIG_PATH" \
    -v "$REAL_INSTANCE_REPO":"$DOCKER_INSTANCE_REPO" \
    -v "$REAL_CREDENTIALS_PATH":"$DOCKER_CREDENTIALS_PATH" \
    "${docker_image}" "${docker_cmd[@]}"
  docker wait "${docker_image}"

  echo "Checking the folder /fbpcs_instances"
  docker run -e FBPCS_GRAPH_API_TOKEN \
    -v "$real_config_path":"$DOCKER_CONFIG_PATH" \
    -v "$REAL_INSTANCE_REPO":"$DOCKER_INSTANCE_REPO" \
    -v "$REAL_CREDENTIALS_PATH":"$DOCKER_CREDENTIALS_PATH" \
    "${docker_image}" "ls -l /fbpcs_instances ; cat /fbpcs_instances/output.txt"
  docker wait "${docker_image}"

  echo "Validating output.txt logs"
  docker run -e FBPCS_GRAPH_API_TOKEN --rm \
    -v "$real_config_path":"$DOCKER_CONFIG_PATH" \
    -v "$REAL_INSTANCE_REPO":"$DOCKER_INSTANCE_REPO" \
    -v "$REAL_CREDENTIALS_PATH":"$DOCKER_CREDENTIALS_PATH" \
    "${docker_image}" "./fbpcs/infra/logging_service/log_analyzer/log_analyzer.sh /fbpcs_instances/output.txt --validate_one_runner_logs"
}

retry_run_fbpcs() {
  exit_code="$?"
  case $exit_code in
    65)
      tag="rc"
      ;;
    66)
      tag="canary"
      ;;
    67)
      tag="latest"
      ;;
    *)
      exit $exit_code
      ;;
  esac

  echo "Overriding docker version tag and config.yml binary tag with $tag"
  docker_image="${FBPCS_CONTAINER_REPO_URL}/${FBPCS_IMAGE_NAME}:${tag}"
  replace_config_var binary_version "$tag" "$real_config_path"

  run_fbpcs
}

main "$@"
