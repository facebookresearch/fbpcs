#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

"""
CLI for analyzing output logs from private_computation_cli execution, and generating digest report in JSON format


Usage:
    log_analyzer <logs_file_to_analyze> [options]


Options:
    -h --help                       Show this help
    --log_path=<path>               Override the default path where logs are saved
    --out=<output_json_file>        Output the digest to a JSON file. By default the summary is written to the log.
    --verbose                       Set logging level to DEBUG
"""

from __future__ import annotations

import logging
import re
import sys
import time

from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Callable, Dict, List, Match, Optional, Set

import schema
from docopt import docopt
from fbpcs.infra.logging_service.log_analyzer.entity.cell_objective_instance import (
    CellObjectiveInstance,
)
from fbpcs.infra.logging_service.log_analyzer.entity.container_info import ContainerInfo
from fbpcs.infra.logging_service.log_analyzer.entity.flow_stage import FlowStage
from fbpcs.infra.logging_service.log_analyzer.entity.instance_flow import InstanceFlow
from fbpcs.infra.logging_service.log_analyzer.entity.log_context import LogContext
from fbpcs.infra.logging_service.log_analyzer.entity.run_study import RunStudy


@dataclass
class ParsingState:
    handler: Callable[
        [LogContext, Match[str], Optional[List[str]]], Optional[ParsingState]
    ]
    context: LogContext
    last_lines: List[str] = field(default_factory=list)


@dataclass
class PatternAndHandler:
    pattern: str
    handler: Callable[
        [LogContext, Match[str], Optional[List[str]]],
        Optional[ParsingState],
    ]


class LogDigest:
    def __init__(
        self,
        logs_file: Path,
        logger: logging.Logger,
    ) -> None:
        self.logger = logger
        self.logs_file = logs_file
        self.run_study: RunStudy = RunStudy(0)
        self.start_epoch_time: str = ""
        self.container_ids: Dict[str, Set[str]] = {}

    def analyze_logs(
        self,
    ) -> RunStudy:
        line_num = 0
        parser_state: Optional[ParsingState] = None
        with open(self.logs_file) as infile:
            for log_line in infile:
                # Read a line and remove any trailing whitespace or newline chars
                line_num += 1
                parser_state = self._parse_one_line(
                    line_num, log_line.rstrip(), parser_state
                )

        self.run_study.total_line_num = line_num
        self._aggregate_summary()
        return self.run_study

    def _aggregate_summary(
        self,
    ) -> None:
        for instance_id in self.container_ids:
            instance_flow = self.run_study.instances[instance_id]
            instance_flow.instance_container_count = len(
                self.container_ids[instance_id]
            )
            # Make the summary of the stages in the instance
            stage_ids = []
            for stage in instance_flow.stages:
                elapsed_hms = "n/a"
                if stage.context.elapsed_second:
                    s = int(float(stage.context.elapsed_second))
                    elapsed_hms = "{:d}h{:02d}m{:02d}s".format(
                        s // 3600, s % 3600 // 60, s % 60
                    )
                instance_flow.summary_stages.append(
                    f"{stage.stage_id}: failed={stage.failed_container_count},"
                    f" end_elapsed={elapsed_hms}, end_update_line={stage.context.line_num}"
                )
                stage_ids.append(stage.stage_id)
            # Make the summary of the instance
            self.run_study.summary_instances.append(
                f"i={instance_id}/o={instance_flow.objective_id}/c={instance_flow.cell_id}: failed_container_count={instance_flow.instance_failed_container_count}, last_stages={stage_ids[-3:]}"
            )

    def _parse_one_line(
        self,
        line_num: int,
        log_line: str,
        parsing_state: Optional[ParsingState],
    ) -> Optional[ParsingState]:
        pattern_handlers = [
            PatternAndHandler(
                # E.g. Created instance 252502207342908 for cell 451002203420028 and objective 159502204793395
                r"Created instance ([^ ]+) for cell ([^ ]+) and objective ([^ ]+)$",
                self._add_created_instance_objective_cell,
            ),
            PatternAndHandler(
                # E.g.: ... Instances to run for cell-obj pairs:
                # {
                #     "7595610074714724": {
                #         "25065264566973790": {
                #             "input_path": "https://fbpcs-github-e2e.s3.us-west-2.amazonaws.com/lift/inputs/partner_e2e_input.csv",
                #             "instance_id": "7540993020268572",
                #             "latest_data_ts": 1647202674,
                #             "num_shards": 1,
                #             "status": "CREATED"
                #         }
                #     }
                # }
                r"Instances to run for cell-obj pairs:",
                self._add_existing_instance,
            ),
            PatternAndHandler(
                # E.g. [252502207342908] Valid stage found: PrivateComputationStageFlow.PID_SHARD
                r"\[([^ ]+)\] Valid stage found: PrivateComputationStageFlow\.([^ ]+)$",
                self._add_flow_stage,
            ),
            PatternAndHandler(
                # E.g. [4547351303806882] {"input_path": ... "status_update_ts": 1648146505, ... }
                # Also have to contain like: "role": "PARTNER"
                r"\[([^ ]+)\] {(?=.*\"role\": \"PARTNER\".*)(\".*status_update_ts\": (\d+).+)}$",
                self._add_containers_from_status_update,
            ),
        ]

        if line_num == 1:
            context = self._parse_line_context(log_line)
            self.run_study.first_log = log_line
            self.run_study.start_epoch_time = context.epoch_time

        # E.g. any of the following cases (incomplete list):
        # 2022-06-06 20:12:54,535Z ERROR t:MainThread n:__main__ ! [7540993020268572] Error: type: ...
        # 2022-06-06 20:16:23,432Z ERROR t:MainThread n:root ! instance_id='7540993020268572' FAILED.
        # ERROR:__main__:[15398047007316153] Error: type: ...
        # ERROR:__main__:instance_id='15398047007316153' FAILED.
        if match := re.search(
            r"^(.{16}:\d{2},\d{3}Z ERROR t:[^!]+! |ERROR:[^:]+:)(.+)$", log_line
        ):
            self._add_line_with_error_log_level(line_num, match)

        if parsing_state:
            # Match the whole log line
            match = re.search(r".*", log_line)
            return parsing_state.handler(
                parsing_state.context, match or Match(), parsing_state.last_lines
            )

        for pattern_handler in pattern_handlers:
            if match := re.search(pattern_handler.pattern, log_line):
                context = self._parse_line_context(log_line)
                context.line_num = line_num
                return pattern_handler.handler(context, match, None)

    def _parse_line_context(
        self,
        log_line: str,
    ) -> LogContext:
        # A log line might start with UTC timestamp, e.g. "2022-05-31 20:59:25,169Z ..."
        match = re.search(r"^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2},\d+)Z ", log_line)
        if not match:
            return LogContext(line_num=0)
        # like "2022-05-31 20:59:25.169"
        ts_str = match.group(1).replace(",", ".")
        ts_datetime = datetime.fromisoformat(ts_str)
        epoch_time = ts_datetime.timestamp()
        elapsed_second = (
            "%.3f" % (epoch_time - float(self.run_study.start_epoch_time))
            if self.run_study.start_epoch_time
            else "0"
        )
        return LogContext(
            line_num=0,
            elapsed_second=elapsed_second,
            epoch_time="%.3f" % epoch_time,
            utc_time=ts_str,
        )

    def _add_line_with_error_log_level(
        self,
        line_num: int,
        match: Match[str],
    ) -> None:
        error_text = match.group(2)
        log_line = match.group(0)
        self.logger.info(
            f"Adding error line: line_num={line_num}, error_text={log_line}"
        )
        # Try to extract instance ID
        match_instance_id = re.search(r"^\[(\d+)\] ", error_text)
        instance_id = match_instance_id.group(1) if match_instance_id else None
        instance_flow = (
            self.run_study.instances.get(instance_id) if instance_id else None
        )
        if instance_flow:
            instance_flow.instance_error_line_count += 1
            instance_flow.instance_error_lines.append(f"{line_num}: {log_line}")
        else:
            self.run_study.error_line_count += 1
            self.run_study.error_lines.append(f"{line_num}: {log_line}")

    def _add_created_instance_objective_cell(
        self,
        context: LogContext,
        match: Match[str],
        _last_lines: Optional[List[str]],
    ) -> None:
        instance_id = match.group(1)
        cell_id = match.group(2)
        objective_id = match.group(3)
        self.logger.info(
            f"Adding newly-created instance: objective={objective_id}, instance={instance_id}, cell={cell_id}. At line_num={context.line_num}"
        )
        self.run_study.instances[match.group(1)] = InstanceFlow(
            context=context,
            instance_id=instance_id,
            objective_id=objective_id,
            cell_id=cell_id,
        )
        self.container_ids[instance_id] = set()

    def _add_existing_instance(
        self,
        context: LogContext,
        match: Match[str],
        last_lines: Optional[List[str]],
    ) -> Optional[ParsingState]:
        if not last_lines:
            # The following lines have JSON data for cell_obj_instance
            self.logger.info("Found cell_obj_instance: start.")
            return ParsingState(
                handler=self._add_existing_instance, context=context, last_lines=[" "]
            )

        log_line = match.group(0)
        last_lines.append(log_line)
        if log_line != "}":
            return ParsingState(
                handler=self._add_existing_instance,
                context=context,
                last_lines=last_lines,
            )

        json_str = " ".join(last_lines)
        self.logger.info(f"Found cell_obj_instance: JSON={json_str}.")
        # pyre-ignore
        cells = CellObjectiveInstance.from_json(f'{{"data": {json_str}}}')
        self.logger.info(f"Found cell_obj_instance: cells={cells}.")

        for cell_id in cells.data:
            objectives = cells.data[cell_id]
            for objective_id in objectives:
                instance = objectives[objective_id]
                instance_id = instance.get("instance_id")
                if not instance_id:
                    continue
                if instance_id in self.run_study.instances:
                    self.logger.info(
                        f"Skipped already-added instance: objective={objective_id}, instance={instance_id}, cell={cell_id}. At line_num={context.line_num}"
                    )
                    continue

                self.logger.info(
                    f"Adding existing instance: objective={objective_id}, instance={instance_id}, cell={cell_id}. At line_num={context.line_num}"
                )
                self.run_study.instances[instance_id] = InstanceFlow(
                    context=context,
                    instance_id=instance_id,
                    objective_id=objective_id,
                    cell_id=cell_id,
                    existing_instance_status=instance.get("status"),
                )
                self.container_ids[instance_id] = set()

        return None

    def _add_flow_stage(
        self,
        context: LogContext,
        match: Match[str],
        _last_lines: Optional[List[str]],
    ) -> None:
        # The stage ID's are like PC_PRE_VALIDATION, PID_SHARD, etc.
        # They also appear in the log lines highlighting the current stage among the full flow. E.g.
        # CREATED -> PC_PRE_VALIDATION -> [**PID_SHARD**] -> PID_PREPARE -> ID_MATCH -> ID_MATCH_POST_PROCESS -> PREPARE -> COMPUTE -> AGGREGATE -> POST_PROCESSING_HANDLERS
        self.logger.info(
            f"Found flow_stage={match.group(2)}, instance={match.group(1)}. At line_num={context.line_num}"
        )
        self.run_study.instances[match.group(1)].stages.append(
            FlowStage(
                context=context,
                stage_id=match.group(2),
            )
        )

    def _add_containers_from_status_update(
        self,
        context: LogContext,
        match: Match[str],
        _last_lines: Optional[List[str]],
    ) -> None:
        instance_id = match.group(1)
        # This is from entity type PrivateComputationInstance
        instance_data = match.group(2)
        epoch_second = match.group(3)
        self.logger.info(
            f"Found status update: instance={instance_id}, epoch_time={epoch_second}. At line_num={context.line_num}"
        )

        # Fill the timestamp in context when the context has no timestamp.
        if not context.epoch_time:
            # like "1654147049.156"
            context.epoch_time = epoch_second + ".000"
            ts_datetime = datetime.utcfromtimestamp(int(epoch_second))
            # like "2022-05-31 20:59:25.169"
            context.utc_time = ts_datetime.isoformat().replace("T", " ") + ".000"

        # Firstly handle all special cases in the order of stages in the flow
        self._try_add_containers_in_runpid_stage(context, instance_id, instance_data)

        # Finally handle all containers
        self._add_containers_to_last_stage(
            instance_id,
            self._extract_new_containers(
                instance_id,
                instance_data,
                context,
            ),
        )

    def _try_add_containers_in_runpid_stage(
        self,
        context: LogContext,
        instance_id: str,
        instance_data: str,
    ) -> None:
        # try to find the containers in "stages_containers" in the ADV_RUN_PID stage
        if match := re.search(
            r"\"stages_containers\": {\"ADV_SHARD\": \[([^\]]+)\]",
            instance_data,
        ):
            self._add_containers_to_last_stage(
                instance_id,
                self._extract_new_containers(
                    instance_id,
                    match.group(1),
                    context,
                ),
                ["ADV_SHARD"],
            )

            match = re.search(r"\"ADV_PREPARE\": \[([^\]]+)\]", instance_data)
            if match:
                self._add_containers_to_last_stage(
                    instance_id,
                    self._extract_new_containers(
                        instance_id,
                        match.group(1),
                        context,
                    ),
                    ["ADV_PREPARE"],
                )

            match = re.search(r"\"ADV_RUN_PID\": \[([^\]]+)\]", instance_data)
            if match:
                self._add_containers_to_last_stage(
                    instance_id,
                    self._extract_new_containers(
                        instance_id,
                        match.group(1),
                        context,
                    ),
                    ["ADV_RUN_PID"],
                )

    def _add_containers_to_last_stage(
        self,
        instance_id: str,
        containers: List[ContainerInfo],
        stage_tags: Optional[List[str]] = None,
    ) -> None:
        instance_flow = self.run_study.instances.get(instance_id)
        if not instance_flow:
            return
        last_stage = instance_flow.stages[-1] if instance_flow.stages else None
        if not last_stage:
            return
        self.logger.info(f"Adding container count={len(containers)}")
        for container in containers:
            self.container_ids[instance_id].add(container.container_id)
            self.logger.info(
                f"Adding container={container.container_id}. At line_num={container.context.line_num}"
            )
            last_stage.containers.append(container)
            last_stage.container_count = len(last_stage.containers)
            if container.status == "FAILED":
                last_stage.failed_container_count -= 1
                instance_flow.instance_failed_container_count -= 1

        if containers and stage_tags:
            last_stage.stage_tags = stage_tags

    def _extract_new_containers(
        self,
        instance_id: str,
        log_str: str,
        context: LogContext,
    ) -> List[ContainerInfo]:
        added_container_ids = self.container_ids[instance_id]
        containers: List[ContainerInfo] = []
        id_list = re.findall(r"\"instance_id\": \"(arn:[^\"]+)\"", log_str)
        status_list = re.findall(r"\"status\": \"([^\"]+)\"", log_str)
        # log_url might be missing, e.g. when running computation client against dev account.
        log_url_list = re.findall(r"\"log_url\": \"([^\"]+)\"", log_str) or [
            None
        ] * len(id_list)
        for id, status, log_url in zip(id_list, status_list, log_url_list):
            if id in added_container_ids:
                continue  # this container is added before and ignored here
            self.logger.debug(
                f"Found new container=[{instance_id}]: {{{id}, {status}, {log_url}}}"
            )
            containers.append(
                ContainerInfo(
                    context=context,
                    container_id=id,
                    status=status,
                    log_url=log_url,
                )
            )

        return containers


def main(argv: Optional[List[str]] = None) -> None:
    s = schema.Schema(
        {
            "<logs_file_to_analyze>": schema.Use(Path),
            "--log_path": schema.Or(None, schema.Use(Path)),
            "--out": schema.Or(None, schema.Use(Path)),
            "--verbose": bool,
            "--help": bool,
        }
    )

    arguments = s.validate(docopt(__doc__, argv))

    logs_file = arguments["<logs_file_to_analyze>"]
    log_path = arguments["--log_path"]
    output_json_path = arguments["--out"]

    # if log_path specified, logging using FileHandler, or console StreamHandler
    log_handler = logging.FileHandler(log_path) if log_path else logging.StreamHandler()
    logging.Formatter.converter = time.gmtime
    logging.basicConfig(
        level=logging.INFO,
        handlers=[log_handler],
        format="%(asctime)sZ %(levelname)s t:%(threadName)s n:%(name)s ! %(message)s",
    )
    logger = logging.getLogger(__name__)
    log_level = logging.DEBUG if arguments["--verbose"] else logging.INFO
    logger.setLevel(log_level)
    # Concatenate all arguments to a string, with every argument wrapped by quotes.
    all_options = f"{sys.argv[1:]}"[1:-1].replace("', '", "' '")
    # E.g. Command line: log_analyzer 'sample_log/intern-output.txt' '--log_path=a.intern.log' ...
    logging.info(f"Command line: {Path(__file__).stem} {all_options}")

    digest = LogDigest(logs_file, logger)
    run_study = digest.analyze_logs()
    logger.info(f"Parsed log line count: {run_study.total_line_num}")
    # pyre-ignore
    summary_json = run_study.to_json(indent=4)
    if output_json_path:
        with open(output_json_path, "w") as outfile:
            outfile.write(summary_json)
    else:
        logger.info(f"Generated run study digest:\n{summary_json}")
    logger.info(f"Done. Instance count: {len(run_study.instances)}")


if __name__ == "__main__":
    main()
