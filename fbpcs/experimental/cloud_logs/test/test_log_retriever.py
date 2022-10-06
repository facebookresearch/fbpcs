#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

import unittest

from fbpcs.experimental.cloud_logs.aws_log_retriever import (
    AWSLogRetriever,
    LogGroupGuessStrategy,
)


class TestLogRetriever(unittest.TestCase):
    def test_aws_invalid_container_id(self) -> None:
        retriever = AWSLogRetriever()

        with self.assertRaises(IndexError):
            retriever.get_log_url("aaaaaaaaaaaaaaa")

    def test_get_log_url_shared_log_group_on_publisher(self) -> None:
        retriever = AWSLogRetriever(
            log_group_guess_strategy=LogGroupGuessStrategy.FROM_PCE_SERVICE
        )
        container_id = "arn:aws:ecs:us-west-2:539290649537:task/onedocker-cluster-ee9bc805f22e40f9bbc107d5f006b6e1/3a5e4213036b4456a6c16695b938b361"
        expected = "https://us-west-2.console.aws.amazon.com/cloudwatch/home?region=us-west-2#logsV2:log-groups/log-group/$252Fecs$252Fonedocker-container-shared-us-west-2/log-events/ecs$252Fonedocker-container-shared-us-west-2$252F3a5e4213036b4456a6c16695b938b361"
        actual = retriever.get_log_url(container_id)
        self.assertEqual(expected, actual)

    def test_ep_publisher(self) -> None:
        retriever = AWSLogRetriever(
            awslogs_group="/ecs/onedocker-container-shared-us-west-2"
        )
        container_id = "arn:aws:ecs:us-west-2:119557546360:task/onedocker-cluster-mpc-aem-exp-platform-publisher/aeb6151d016046dab698b988e04018d4"
        expected_url = "https://us-west-2.console.aws.amazon.com/cloudwatch/home?region=us-west-2#logsV2:log-groups/log-group/$252Fecs$252Fonedocker-container-shared-us-west-2/log-events/ecs$252Fonedocker-container-shared-us-west-2$252Faeb6151d016046dab698b988e04018d4"
        actual = retriever.get_log_url(container_id)
        self.assertEqual(expected_url, actual)

    def test_ep_partner(self) -> None:
        retriever = AWSLogRetriever()
        container_id = "arn:aws:ecs:us-west-2:119557546360:task/onedocker-cluster-mpc-aem-exp-platform-partner/dc4fdf05e7684165b639b4e1831872c8"
        expected_url = "https://us-west-2.console.aws.amazon.com/cloudwatch/home?region=us-west-2#logsV2:log-groups/log-group/$252Fecs$252Fonedocker-container-mpc-aem-exp-platform-partner/log-events/ecs$252Fonedocker-container-mpc-aem-exp-platform-partner$252Fdc4fdf05e7684165b639b4e1831872c8"
        actual = retriever.get_log_url(container_id)
        self.assertEqual(expected_url, actual)
