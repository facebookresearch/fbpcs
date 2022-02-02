#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

import unittest

from fbpcs.experimental.cloud_logs.log_retriever import CloudProvider, LogRetriever


class TestLogRetriever(unittest.TestCase):
    def test_get_aws_cloudwatch_log_url(self) -> None:
        retriever = LogRetriever(CloudProvider.AWS)
        container_id = "arn:aws:ecs:us-west-2:539290649537:task/onedocker-cluster-fake-amazon/3a5e4213036b4456a6c16695b938b361"
        expected = "https://us-west-2.console.aws.amazon.com/cloudwatch/home?region=us-west-2#logsV2:log-groups/log-group/$252Fecs$252Fonedocker-container-fake-amazon/log-events/ecs$252Fonedocker-container-fake-amazon$252F3a5e4213036b4456a6c16695b938b361"
        actual = retriever._get_aws_cloudwatch_log_url(container_id)
        self.assertEqual(expected, actual)

        with self.assertRaises(IndexError):
            retriever._get_aws_cloudwatch_log_url("aaaaaaaaaaaaaaa")

    def test_get_log_url_aws_provider(self) -> None:
        retriever = LogRetriever(CloudProvider.AWS)
        container_id = "arn:aws:ecs:us-west-2:539290649537:task/onedocker-cluster-fake-amazon/3a5e4213036b4456a6c16695b938b361"
        expected = "https://us-west-2.console.aws.amazon.com/cloudwatch/home?region=us-west-2#logsV2:log-groups/log-group/$252Fecs$252Fonedocker-container-fake-amazon/log-events/ecs$252Fonedocker-container-fake-amazon$252F3a5e4213036b4456a6c16695b938b361"
        actual = retriever.get_log_url(container_id)
        self.assertEqual(expected, actual)

        with self.assertRaises(IndexError):
            retriever.get_log_url("aaaaaaaaaaaaaaa")

    def test_get_log_url_gcp_provider(self) -> None:
        retriever = LogRetriever(CloudProvider.GCP)
        container_id = "fancy_container"
        with self.assertRaises(NotImplementedError):
            retriever.get_log_url(container_id)
