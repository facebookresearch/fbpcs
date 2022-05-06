#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

import logging
from typing import Any, Dict, List

from fbpcs.private_computation_cli.private_computation_service_wrapper import (
    _build_private_computation_service,
)


def pre_validate(
    config: Dict[str, Any],
    input_paths: List[str],
    logger: logging.Logger,
) -> None:
    pc_service = _build_private_computation_service(
        config["private_computation"],
        config["mpc"],
        config["pid"],
        config.get("post_processing_handlers", {}),
        config.get("pid_post_processing_handlers", {}),
    )
    paths_string = "\n".join(input_paths)
    logger.info(f"Starting pre_validate on input_paths: {paths_string}")
    assert pc_service
