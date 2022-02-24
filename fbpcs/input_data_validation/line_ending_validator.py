# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict


from fbpcs.input_data_validation.constants import VALID_LINE_ENDING_REGEX


class LineEndingValidator:
    def validate(self, line: str) -> None:
        if not VALID_LINE_ENDING_REGEX.match(line):
            raise Exception(
                "Detected an unexpected line ending. The only supported line ending is '\\n'"
            )
