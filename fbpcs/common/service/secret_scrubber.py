#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

import logging
import re
from dataclasses import dataclass
from typing import Dict, List


@dataclass
class Secret:
    name: str
    regex_pattern_str: str


@dataclass
class ScrubSummary:
    scrubbed_output: str
    total_substitutions: int
    name_to_num_subs: Dict[str, int]

    def get_report(self) -> str:
        report = "Scrub Summary\n"
        for name, num_subs in self.name_to_num_subs.items():
            report += f"\n* {name}: {num_subs} substitutions"
        report += f"\n* Total substitutions: {self.total_substitutions}"
        return report


class SecretScrubber:
    SECRETS: List[Secret] = [
        # https://aws.amazon.com/blogs/security/a-safer-way-to-distribute-aws-credentials-to-ec2/
        Secret("AWS access key id", "(?<![A-Z0-9])[A-Z0-9]{20}(?![A-Z0-9])"),
        # https://aws.amazon.com/blogs/security/a-safer-way-to-distribute-aws-credentials-to-ec2/
        Secret(
            "AWS secret access key",
            "(?<![A-Za-z0-9/+=])[A-Za-z0-9/+=]{40}(?![A-Za-z0-9/+=])",
        ),
        # https://fburl.com/code/ogf6f0v4
        Secret("Meta Graph API token", "([^a-zA-Z0-9]|^)EAA[a-zA-Z0-9]{90,400}"),
    ]
    REPLACEMENT_STR: str = "********"

    def __init__(self) -> None:
        self.patterns: Dict[str, re.Pattern] = {
            secret.name: re.compile(secret.regex_pattern_str) for secret in self.SECRETS
        }

    def scrub(self, string: str) -> ScrubSummary:
        total_substitutions = 0
        name_to_num_subs = {}
        for name, pattern in self.patterns.items():
            string, num_substitutes = pattern.subn(self.REPLACEMENT_STR, string)
            name_to_num_subs[name] = num_substitutes
            total_substitutions += num_substitutes
        return ScrubSummary(string, total_substitutions, name_to_num_subs)


class LoggingSecretScrubber(logging.Formatter):
    SECRET_SCRUBBER: SecretScrubber = SecretScrubber()

    def format(self, record: logging.LogRecord) -> str:
        original = logging.Formatter.format(self, record)
        return self.SECRET_SCRUBBER.scrub(original).scrubbed_output
