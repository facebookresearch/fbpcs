#!/usr/bin/env python3
# Copyright (c) Facebook, Inc. and its affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

"""
Coordinator for PID protocol. Handles prepare, match, and combine.

Usage:
    pid_coordinator shard <input_path> <output_path> --num_shards=<n> [options]
    pid_coordinator prepare <input_path> <output_path> --num_shards=<n> [options]
    pid_coordinator shuffler publisher --config=<cfg> <input_path> <output_path> <ip_file> --encryption_keys=<encryption_keys> --num_shards=<n> [options]
    pid_coordinator shuffler partner --config=<cfg> <input_path> <output_path> <ip_file> --encryption_keys=<encryption_keys> --num_shards=<n> [options]
    pid_coordinator run_mk publisher --config=<cfg> <input_path> <output_path> <ip_file> --num_shards=<n> [options]
    pid_coordinator run_mk partner --config=<cfg> <input_path> <output_path> <ip_file> --num_shards=<n> [options]
    pid_coordinator run publisher --config=<cfg> <input_path> <output_path> <ip_file> --num_shards=<n> [options]
    pid_coordinator run partner --config=<cfg> <input_path> <output_path> <ip_file> --num_shards=<n> [options]
    pid_coordinator combine <spine_path> <data_path> <output_path> --num_shards=<n> [options]
    pid_coordinator aggregate <input_path> <output_path> --num_shards=<n> [options]

Options:
    -h --help          Print this help text
    -p --port          Override the default port for PID connections [default: 15200]
    -v --verbose       Set logging level to DEBUG instead of INFO
"""

import json
import os
import pathlib
import re
import shlex
import subprocess
import tempfile
import time
from typing import Any, Dict, List, Optional

import docopt
import schema
import yaml
from fbpmp.utils.abstract_file_ctx import (
    abstract_file_reader_path,
    abstract_file_writer_ctx,
)


S3_PATH_REGEX = re.compile(r"^https:\/\/(.*).s3.(.*).amazonaws.com\/(.*)$")
SLEEP_TIME = 10
MULTI_KEY_PROTOCOL = "multi_key"
MULTI_KEY_SHUFFLER_PROTOCOL = "multi_key_shuffler"


class UnreachableBlockError(Exception):
    pass


def check_retcode(retcode: int, msg: str) -> None:
    if retcode != 0:
        msg = f"{msg}\n[retcode = {retcode}]"
        raise ValueError(msg)


def https_path_to_s3_path(output_path: str) -> Optional[str]:
    res = S3_PATH_REGEX.match(output_path)
    if not res:
        return None
    bucket = res.group(1)
    # region = res.group(2)
    key = res.group(3)
    return f"s3://{bucket}/{key}"


def gen_vpc(subnet: str, security_group: str) -> str:
    return f'{{"subnets": ["{subnet}"], "securityGroups": ["{security_group}"], "assignPublicIp": "ENABLED"}}'


def gen_command(
    in_path: str,
    out_path: str,
    server_hostname: Optional[str] = None,
    protocol_name: Optional[str] = None,
    encryption_keys: Optional[str] = None,
    port: int = 15300,
) -> str:
    if server_hostname:
        if not protocol_name:
            return f'["python3 -m src.pid_worker.pid_worker run partner {in_path} {out_path} {server_hostname} --port={port}"]'
        elif protocol_name == MULTI_KEY_SHUFFLER_PROTOCOL:
            return f'["python3 -m src.pid_worker.pid_worker shuffler partner {in_path} {out_path} {server_hostname} {encryption_keys} --port={port}"]'
        elif protocol_name == MULTI_KEY_PROTOCOL:
            return f'["python3 -m src.pid_worker.pid_worker run_mk partner {in_path} {out_path} {server_hostname} --port={port}"]'
        else:
            raise ValueError("The protocol is not valid")
    else:
        if not protocol_name:
            return f'["python3 -m src.pid_worker.pid_worker run publisher {in_path} {out_path} --port={port}"]'
        elif protocol_name == MULTI_KEY_SHUFFLER_PROTOCOL:
            return f'["python3 -m src.pid_worker.pid_worker shuffler publisher {in_path} {out_path} {encryption_keys} --port={port}"]'
        elif protocol_name == MULTI_KEY_PROTOCOL:
            return f'["python3 -m src.pid_worker.pid_worker run_mk publisher {in_path} {out_path} --port={port}"]'
        else:
            raise ValueError("The protocol is not valid")


def gen_environment(key_id: str, key_data: str, region: str) -> str:
    env = {
        "PL_AWS_KEY_ID": key_id,
        "PL_AWS_KEY_DATA": key_data,
        "PL_AWS_REGION": region,
    }
    objs = ", ".join(f'{{"name": "{k}", "value": "{v}"}}' for k, v in env.items())
    return "[" + objs + "]"


def gen_cli(config: Dict[str, Any], cluster: str, cmd: str, env: str, vpc: str) -> str:
    task_def = config["pid_dependency"]["task_definition"]
    net_cfg = (
        f'\'{{"awsvpcConfiguration": {vpc}}}\' --overrides \'{{"containerOverrides": '
        f'[{{"name": "pid-container", "command": {cmd}, "environment": {env}}}]}}\''
    )
    return (
        f"aws ecs run-task --task-definition {task_def} "
        f"--cluster {cluster} --network-configuration {net_cfg}"
    )


def get_json_response_for_cmd(cmd: List[str]) -> Dict[str, Any]:
    p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    out, err = p.communicate()
    return json.loads(out)


def spawn_ecs_tasks(
    config: Dict[str, Any],
    input_path: str,
    output_path: str,
    num_shards: int,
    ips: Optional[List[str]] = None,
    protocol_name: Optional[str] = None,
    encryption_keys: Optional[str] = None,
) -> List[subprocess.Popen]:
    cluster = config["pid_dependency"]["ecs"]["cluster"]
    subnet = config["pid_dependency"]["ecs"]["subnet"]
    security_group = config["pid_dependency"]["ecs"]["security_group"]

    key_id = config["pid_dependency"]["s3_creds"]["access_key_id"]
    key_data = config["pid_dependency"]["s3_creds"]["access_key_data"]
    region = config["pid_dependency"]["s3_creds"]["region"]

    vpc = gen_vpc(subnet, security_group)
    env = gen_environment(key_id, key_data, region)
    procs = []
    for i in range(num_shards):
        # Sleep 1 sec between each spawn to avoid sending too many
        # requests at once (and potentially getting blocked)
        time.sleep(1)
        print(f"Spawn task {i+1} / {num_shards}")
        if ips is not None:
            cmd = gen_command(
                f"{input_path}_{i}",
                f"{output_path}_{i}",
                server_hostname=ips[i],
                protocol_name=protocol_name,
                encryption_keys=encryption_keys,
            )
        else:
            cmd = gen_command(
                f"{input_path}_{i}",
                f"{output_path}_{i}",
                protocol_name=protocol_name,
                encryption_keys=encryption_keys,
            )
        escaped_cmd = shlex.split(gen_cli(config, cluster, cmd, env, vpc))
        procs.append(
            subprocess.Popen(
                escaped_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE
            )
        )
    return procs


def get_arns_for_tasks(procs: List[subprocess.Popen]) -> List[str]:
    arns = []
    for i, proc in enumerate(procs):
        out, err = proc.communicate()
        try:
            out_json = json.loads(out)
            arn = out_json["tasks"][0]["taskArn"]
            arns.append(arn)
        except Exception:
            raise ValueError(
                f"Failed to get ARN for task {i}. This is an unrecoverable error.\n"
                f"Response returned from task start:\n\n{out}\n\n"
            )
    return arns


def get_ips_from_arns(config: Dict[str, Any], arns: List[str]) -> List[str]:
    cluster = config["pid_dependency"]["ecs"]["cluster"]
    ips = []
    for i, arn in enumerate(arns):
        # Try to get the IPv4 every 30 seconds
        cmd = shlex.split(f"aws ecs describe-tasks --cluster {cluster} --tasks {arn}")
        ipv4 = ""
        found_ip = False
        ready = False
        while not (found_ip and ready):
            try:
                out_json = get_json_response_for_cmd(cmd)
                container = out_json["tasks"][0]["containers"][0]
                ipv4 = container["networkInterfaces"][0]["privateIpv4Address"]
                if not found_ip:
                    found_ip = True
                    print(f"Found IP({ipv4}) for task {i}")

                last_status = container["lastStatus"]
                print(f"Task[{i}] (IPv4={ipv4}) has status {last_status}")
                ready = last_status == "RUNNING"
                if last_status == "STOPPED":
                    raise ValueError(f"Task {i} stopped for an unknown reason")
            except (KeyError, IndexError):
                print(
                    f"IP for task {i} not yet ready. Sleeping {SLEEP_TIME} seconds..."
                )
                time.sleep(SLEEP_TIME)

            if not ready:
                print(f"Task {i} is not ready. Sleeping {SLEEP_TIME} seconds...")
                time.sleep(SLEEP_TIME)
        ips.append(ipv4)
    return ips


def upload_files(files: List[str], s3_path: str) -> None:
    # Upload to S3 if not given a local path
    print("Now uploading files to S3...")
    for i, fn in enumerate(files):
        cmd = shlex.split(f'aws s3 cp "{fn}" "{s3_path}_{i}"')
        print(f"Upload shard [{i}]: {cmd}")
        # Intentionally run sequentially to avoid potential upload errors
        retcode = subprocess.Popen(cmd).wait()
        check_retcode(retcode, f"Upload[{i}] raise an error")


def shard(input_path: str, output_path: str, num_shards: int) -> None:
    s3_outpath = https_path_to_s3_path(output_path)
    local_input = abstract_file_reader_path(pathlib.Path(input_path))
    sharder_exe = os.environ["CPP_SHARDER_HASHED_FOR_PID_PATH"]
    cmd = [
        sharder_exe,
        f"--data_directory={local_input.parent}",
        f"--input_filename={local_input.name}",
        f"--num_shards={num_shards}",
    ]
    proc = subprocess.Popen(cmd)

    retcode = proc.wait()
    check_retcode(retcode, "Sharder raised an error")

    sharded_files = [f"{local_input}_{i}" for i in range(num_shards)]
    if s3_outpath is not None:
        upload_files(sharded_files, s3_outpath)


def prepare(input_path: str, output_path: str, num_shards: int) -> None:
    s3_outpath = https_path_to_s3_path(output_path)
    if s3_outpath is None:
        # If s3_outpath is None, we can output directly to the local disk
        prepare_outpath = pathlib.Path(output_path)
    else:
        with tempfile.NamedTemporaryFile() as f:
            prepare_outpath = pathlib.Path(f.name)

    procs = []
    output_paths = []
    for i in range(num_shards):
        preamble = "python3 -m src.pid_worker.pid_worker prepare"
        local_path = abstract_file_reader_path(pathlib.Path(f"{input_path}_{i}"))
        output_path = f"{prepare_outpath}_{i}"
        cmd = shlex.split(f'{preamble} "{local_path}" "{output_path}"')
        print(f"Running prepare command on shard {i}: {cmd}")
        procs.append(subprocess.Popen(cmd))
        output_paths.append(output_path)

    for i, proc in enumerate(procs):
        retcode = proc.wait()
        check_retcode(retcode, f"Prepare task[{i}] raised an error.")

    if s3_outpath is not None:
        upload_files(output_paths, s3_outpath)


def run_publisher(
    config: Dict[str, Any],
    input_path: str,
    output_path: str,
    ip_file: str,
    num_shards: int,
    protocol_name: Optional[str] = None,
    encryption_keys: Optional[str] = None,
) -> None:
    print("Spawning ECS tasks for publisher")
    procs = spawn_ecs_tasks(
        config,
        input_path,
        output_path,
        num_shards,
        protocol_name=protocol_name,
        encryption_keys=encryption_keys,
    )
    print("Getting ARNs from tasks")
    arns = get_arns_for_tasks(procs)
    print("Collecting IPs for tasks")
    ips = get_ips_from_arns(config, arns)

    print(f"Writing IPs to {ip_file}")
    with abstract_file_writer_ctx(pathlib.Path(ip_file)) as f:
        for ip in ips:
            f.write(f"https://{ip}\n")

    print("Done")


def run_partner(
    config: Dict[str, Any],
    input_path: str,
    output_path: str,
    ip_file: str,
    num_shards: int,
    protocol_name: Optional[str] = None,
    encryption_keys: Optional[str] = None,
) -> None:
    # First load the IPs
    ips = []
    print(f"Loading IPs from {ip_file}")
    local_path = abstract_file_reader_path(pathlib.Path(ip_file))
    with open(local_path) as f:
        for line in f:
            ips.append(line.strip())

    if len(ips) != num_shards:
        raise ValueError(f"Expected {num_shards} IPs but found {len(ips)} in {ip_file}")

    if protocol_name == MULTI_KEY_SHUFFLER_PROTOCOL and not encryption_keys:
        raise ValueError("Expected encryption_keys_path")

    print("Spawning ECS tasks for partner")
    procs = spawn_ecs_tasks(
        config,
        input_path,
        output_path,
        num_shards,
        ips,
        protocol_name=protocol_name,
        encryption_keys=encryption_keys,
    )
    for proc in procs:
        proc.wait()

    print("Done")


def combine(
    spine_path: str,
    data_path: str,
    output_path: str,
    num_shards: int,
) -> None:
    s3_outpath = https_path_to_s3_path(output_path)
    if s3_outpath is None:
        # If s3_outpath is None, we can output directly to the local disk
        outpath = pathlib.Path(output_path)
    else:
        with tempfile.NamedTemporaryFile() as f:
            outpath = pathlib.Path(f.name)

    print("Running combiner")
    procs = []
    for i in range(num_shards):
        this_spine = f"{spine_path}_{i}"
        this_data = f"{data_path}_{i}"
        local_spine = abstract_file_reader_path(pathlib.Path(this_spine))
        local_data = abstract_file_reader_path(pathlib.Path(this_data))
        cmd = shlex.split(
            f'python3 -m src.pid_worker.pid_worker combine "{local_spine}" "{local_data}" "{outpath}_{i}"'
        )
        print(f"Running combine command: {cmd}")
        procs.append(subprocess.Popen(cmd))

    for i, proc in enumerate(procs):
        retcode = proc.wait()
        check_retcode(retcode, f"Combine task[{i}] raised an error.")

    combined_files = [f"{outpath}_{i}" for i in range(num_shards)]
    if s3_outpath is not None:
        upload_files(combined_files, s3_outpath)
    print("Done")


def main():
    args_schema = schema.Schema(
        {
            "shard": bool,
            "prepare": bool,
            "run": bool,
            "run_mk": bool,
            "shuffler": bool,
            "combine": bool,
            "aggregate": bool,
            "publisher": bool,
            "partner": bool,
            "<input_path>": schema.Or(None, str),
            "<output_path>": schema.Or(None, str),
            "<spine_path>": schema.Or(None, str),
            "<data_path>": schema.Or(None, str),
            "<ip_file>": schema.Or(None, schema.Use(pathlib.Path, os.path.exists)),
            "--config": schema.Or(None, schema.Use(pathlib.Path, os.path.exists)),
            "--encryption_keys": schema.Or(
                None, schema.Use(pathlib.Path, os.path.exists)
            ),
            "--num_shards": schema.Use(int),
            "--help": bool,
            "--port": schema.Use(int),
            "--verbose": bool,
        }
    )

    args = args_schema.validate(docopt.docopt(__doc__))
    if args["--config"]:
        with open(args["--config"]) as yml_stream:
            config = yaml.load(yml_stream)

    if args["shard"]:
        shard(
            args["<input_path>"],
            args["<output_path>"],
            args["--num_shards"],
        )
    elif args["prepare"]:
        prepare(
            args["<input_path>"],
            args["<output_path>"],
            args["--num_shards"],
        )
    elif args["shuffler"]:
        if args["publisher"]:
            run_publisher(
                config,
                args["<input_path>"],
                args["<output_path>"],
                args["<ip_file>"],
                args["--num_shards"],
                MULTI_KEY_SHUFFLER_PROTOCOL,
                args["--encryption_keys"],
            )
        elif args["partner"]:
            run_partner(
                config,
                args["<input_path>"],
                args["<output_path>"],
                args["<ip_file>"],
                args["--num_shards"],
                MULTI_KEY_SHUFFLER_PROTOCOL,
                args["--encryption_keys"],
            )
    elif args["run"]:
        if args["publisher"]:
            run_publisher(
                config,
                args["<input_path>"],
                args["<output_path>"],
                args["<ip_file>"],
                args["--num_shards"],
            )
        elif args["partner"]:
            run_partner(
                config,
                args["<input_path>"],
                args["<output_path>"],
                args["<ip_file>"],
                args["--num_shards"],
            )
    elif args["run_mk"]:
        if args["publisher"]:
            run_publisher(
                config,
                args["<input_path>"],
                args["<output_path>"],
                args["<ip_file>"],
                args["--num_shards"],
                MULTI_KEY_PROTOCOL,
            )
        elif args["partner"]:
            run_partner(
                config,
                args["<input_path>"],
                args["<output_path>"],
                args["<ip_file>"],
                args["--num_shards"],
                MULTI_KEY_PROTOCOL,
            )
        else:
            raise UnreachableBlockError()
    elif args["combine"]:
        combine(
            args["<spine_path>"],
            args["<data_path>"],
            args["<output_path>"],
            args["--num_shards"],
        )
    elif args["aggregate"]:
        # Download all files to local disk
        # Concat and sort
        pass
    else:
        raise UnreachableBlockError()


if __name__ == "__main__":
    main()
