# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

import argparse

import yaml


def parse_task_definition_arn(task_definition_arn: str) -> str:
    """
    Parses an AWS task definition ARN and outputs a OneDocker Service style task definition and container definition \
        (task_definition#container)
    Args:
        task_definition_arn: A standard AWS task definition arn

    Returns:
        A string in the format of task_definition#container.
    """
    task_definition_arn_list = task_definition_arn.split(":")
    full_task_name = task_definition_arn_list[-2]
    task_version = task_definition_arn_list[-1]
    full_task_name_list = full_task_name.split("/")
    return f"{full_task_name_list[-1]}:{task_version}#"


def main():
    parser = argparse.ArgumentParser(description='Update the task definition in a PC configuration file')
    parser.add_argument('config_file', type=str, nargs='1',
                        help='The path to the configuration file to update')
    parser.add_argument('task_definition_arn', type=str, nargs='1',
                        help='The new task definition arn to use.')

    args = parser.parse_args()

    task_definition = parse_task_definition_arn(args.task_definition_arn)

    with open(args.config_file, "rw") as config_stream:
        try:
            config_yaml = yaml.safe_load(config_stream)
            config_yaml["private_computation"]["OnedockerServiceConfig"]["constructor"]["task_definition"] = task_definition
        except yaml.YAMLError as exc:
            print(exc)


if __name__ == "__main__":
    main()
