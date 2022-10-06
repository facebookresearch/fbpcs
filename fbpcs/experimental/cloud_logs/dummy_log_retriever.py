#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict
from fbpcs.experimental.cloud_logs.log_retriever import LogRetriever


class DummyLogRetriever(LogRetriever):
    def get_log_url(self, container_id: str) -> str:
        return ""
