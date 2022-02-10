#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

import pathlib
from typing import Optional

from fbpcs.utils import abstract_file_ctx


def test_s3_file_helper() -> None:
    # NOTE: os.environ must set for PL_AWS_* to instantiate S3StorageService
    s3_path = pathlib.Path("s3://file_helper_example/test_file")

    print("Start writer")
    with abstract_file_ctx.abstract_file_writer_ctx(s3_path) as writer:
        writer.write("abcdef")
    print("Writer done")

    content: Optional[str] = None
    print("Start reader")
    with abstract_file_ctx.abstract_file_reader_path(s3_path) as reader:
        content = reader.read_text()
    print("Reader done")

    assert content == "abcdef"


if __name__ == "__main__":
    test_s3_file_helper()
