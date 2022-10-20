#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

import re
from dataclasses import dataclass
from typing import Dict, List, Optional

from fbpcs.common.service.secret_scrubber import ScrubSummary, SecretScrubber


@dataclass
class Secret:
    name: str
    regex_pattern_str: str
    regex_group: int


class PiiLoggingScrubber(SecretScrubber):
    PII_SCRUBBER: List[Secret] = [
        # SHA256 REGEX from https://fburl.com/diffusion/vyfpr5ld
        Secret("sha256", r"[a-fA-F0-9]{64}", 0),
        # EMAIL_REGEX from https://fburl.com/qs04uh7n
        Secret(
            "Email",
            r"([a-z0-9!#\$%&\'\*\+/=\?^_`\{\|\}~-]+(?:(?:\.|%2(E|e))[a-z0-9!#\$%&\'\*\+/=\?^_`\{\|\}~-]+)*(?:%40|@)(?:[a-z0-9](?:[a-z0-9-]*[a-z0-9])?(?:\.|%2(E|e)))+[a-z0-9](?:[a-z0-9-]*[a-z0-9])?)",
            0,
        ),
    ]

    def __init__(self) -> None:
        super().__init__()
        self.pattern_group: Dict[str, int] = {}
        for secret in self.PII_SCRUBBER:
            self.patterns.update({secret.name: re.compile(secret.regex_pattern_str)})
            self.pattern_group.update({secret.name: secret.regex_group})

    def get_substring_match(
        self, regex_name: str, match_obj: re.Match
    ) -> Optional[str]:
        if match_obj.group() is not None:
            group = self.pattern_group.get(regex_name, 0)
            return match_obj.group(group)

    def scrub(self, string: str) -> ScrubSummary:
        total_substitutions: int = 0
        name_to_num_subs: Dict[str, int] = {}
        for name, pattern in self.patterns.items():
            if self.pattern_group.get(name, 0):
                match_obj = re.search(pattern, string)
                if match_obj:
                    replace_string = self.get_substring_match(
                        regex_name=name, match_obj=match_obj
                    )
                    pattern = re.compile(f"{replace_string}")
            string, num_substitutes = re.subn(pattern, self.REPLACEMENT_STR, string)
            name_to_num_subs[name] = num_substitutes
            total_substitutions += num_substitutes
        return ScrubSummary(string, total_substitutions, name_to_num_subs)
