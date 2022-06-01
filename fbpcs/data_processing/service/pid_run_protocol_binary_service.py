#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

from typing import Optional

from fbpcs.onedocker_binary_names import OneDockerBinaryNames

from fbpcs.pid.entity.pid_instance import PIDProtocol
from fbpcs.private_computation.entity.private_computation_instance import (
    PrivateComputationRole,
)
from fbpcs.private_computation.service.run_binary_base_service import (
    RunBinaryBaseService,
)


class PIDRunProtocolBinaryService(RunBinaryBaseService):
    @staticmethod
    def build_args(
        input_path: str,
        output_path: str,
        port: int,
        use_row_numbers: bool = False,
        server_hostname: Optional[str] = None,
        metric_path: Optional[str] = None,
    ) -> str:

        cmd_ls = []

        if server_hostname:
            cmd_ls.append(f"--company {server_hostname}:{port}")
        else:
            cmd_ls.append(f"--host 0.0.0.0:{port}")

        cmd_ls.extend(
            [
                f"--input {input_path}",
                f"--output {output_path}",
            ]
        )

        if metric_path is not None:
            cmd_ls.append(f"--metric-path {metric_path}")
        # later will support TLS/Transport Layer Security
        cmd_ls.append("--no-tls")
        # later will support use-rowk-number feature
        if use_row_numbers:
            cmd_ls.append("--use-row-numbers")

        return " ".join(cmd_ls)

    @staticmethod
    def get_binary_name(protocol: PIDProtocol, pc_role: PrivateComputationRole) -> str:
        if pc_role is PrivateComputationRole.PUBLISHER:
            binary_name = OneDockerBinaryNames.PID_SERVER.value
            if protocol is PIDProtocol.UNION_PID_MULTIKEY:
                binary_name = OneDockerBinaryNames.PID_MULTI_KEY_SERVER.value
        elif pc_role is PrivateComputationRole.PARTNER:
            binary_name = OneDockerBinaryNames.PID_CLIENT.value
            if protocol is PIDProtocol.UNION_PID_MULTIKEY:
                binary_name = OneDockerBinaryNames.PID_MULTI_KEY_CLIENT.value
        else:
            raise ValueError(f"Unsupported PrivateComputationRole passed: {pc_role}")
        return binary_name
