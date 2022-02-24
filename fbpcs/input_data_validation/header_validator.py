# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict


from typing import Sequence

from fbpcs.input_data_validation.constants import PA_FIELDS, PL_FIELDS


class HeaderValidator:
    def validate(self, header_row: Sequence[str]) -> None:
        if not header_row:
            raise Exception("The header row was empty.")

        match_pa_fields = len(set(PA_FIELDS).intersection(set(header_row))) == len(
            PA_FIELDS
        )
        match_pl_fields = len(set(PL_FIELDS).intersection(set(header_row))) == len(
            PL_FIELDS
        )

        if not (match_pa_fields or match_pl_fields):
            raise Exception(
                f"Failed to parse the header row. The header row fields must be either: {PL_FIELDS} or: {PA_FIELDS}"
            )
