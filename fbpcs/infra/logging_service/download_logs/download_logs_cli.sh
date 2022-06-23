#!/bin/bash
# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

# Usage:
#   Get the complete help with -h option.

set -e

script_dir="$( cd -- "$( dirname -- "${BASH_SOURCE[0]:-$0}"; )" &> /dev/null && pwd 2> /dev/null; )";
echo Python packages are required:
cat "$script_dir/pip_requirements.txt"

fbpcs_dir=$(pushd "$script_dir/../../.." > /dev/null ; pwd ; popd > /dev/null)
pythonpath="$PYTHONPATH:$fbpcs_dir/.."
echo Updated PYTHONPATH=\""$pythonpath"\"
export PYTHONPATH="$pythonpath"; python "$script_dir/download_logs_cli.py" "$@"
