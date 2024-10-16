#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

# pyre-unsafe

from dataclasses import dataclass
from typing import Optional

from fbpcs.onedocker_binary_names import OneDockerBinaryNames

from fbpcs.pid.entity.pid_instance import PIDProtocol

# pyre-fixme[21]: Could not find module
#  `fbpcs.private_computation.entity.private_computation_instance`.
from fbpcs.private_computation.entity.private_computation_instance import (
    PrivateComputationRole,
)
from fbpcs.private_computation.service.run_binary_base_service import (
    RunBinaryBaseService,
)


@dataclass
class TlsArgs:
    use_tls: bool
    ca_cert_path: Optional[str] = None
    server_cert_path: Optional[str] = None
    private_key_path: Optional[str] = None


class PIDRunProtocolBinaryService(RunBinaryBaseService):
    @staticmethod
    def build_args(
        input_path: str,
        output_path: str,
        port: int,
        tls_args: TlsArgs,
        # pyre-fixme[11]: Annotation `PrivateComputationRole` is not defined as a type.
        pc_role: PrivateComputationRole,
        use_row_numbers: bool = False,
        server_endpoint: Optional[str] = None,
        metric_path: Optional[str] = None,
        run_id: Optional[str] = None,
    ) -> str:

        cmd_ls = []

        if server_endpoint:
            cmd_ls.append(f"--company {server_endpoint}:{port}")
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

        if not tls_args.use_tls:
            cmd_ls.append("--no-tls")

        # later will support use-rowk-number feature
        if use_row_numbers:
            cmd_ls.append("--use-row-numbers")

        # pyre-fixme[16]: Module `entity` has no attribute
        #  `private_computation_instance`.
        if tls_args.ca_cert_path and pc_role is PrivateComputationRole.PARTNER:
            cmd_ls.append(f"--tls-ca {tls_args.ca_cert_path}")

        # pyre-fixme[16]: Module `entity` has no attribute
        #  `private_computation_instance`.
        if tls_args.server_cert_path and pc_role is PrivateComputationRole.PUBLISHER:
            cmd_ls.append(f"--tls-cert {tls_args.server_cert_path}")

        # pyre-fixme[16]: Module `entity` has no attribute
        #  `private_computation_instance`.
        if tls_args.private_key_path and pc_role is PrivateComputationRole.PUBLISHER:
            cmd_ls.append(f"--tls-key {tls_args.private_key_path}")

        if run_id is not None:
            cmd_ls.append(f"--run_id {run_id}")

        return " ".join(cmd_ls)

    @staticmethod
    def get_binary_name(protocol: PIDProtocol, pc_role: PrivateComputationRole) -> str:
        # pyre-fixme[16]: Module `entity` has no attribute
        #  `private_computation_instance`.
        if pc_role is PrivateComputationRole.PUBLISHER:
            binary_name = OneDockerBinaryNames.PID_SERVER.value
            if protocol is PIDProtocol.UNION_PID_MULTIKEY:
                binary_name = OneDockerBinaryNames.PID_MULTI_KEY_SERVER.value
        # pyre-fixme[16]: Module `entity` has no attribute
        #  `private_computation_instance`.
        elif pc_role is PrivateComputationRole.PARTNER:
            binary_name = OneDockerBinaryNames.PID_CLIENT.value
            if protocol is PIDProtocol.UNION_PID_MULTIKEY:
                binary_name = OneDockerBinaryNames.PID_MULTI_KEY_CLIENT.value
        else:
            raise ValueError(f"Unsupported PrivateComputationRole passed: {pc_role}")
        return binary_name
