#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict


from typing import Dict

from fastapi import APIRouter

router = APIRouter()


@router.get("/")
def hello_world() -> Dict[str, str]:
    return {"message": "Hello World"}
