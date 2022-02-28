#!/bin/bash
# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

set -e

if [ $# -ne 2 ]; then
    echo "Usage: $0 image_name image_tag"
    echo "Example: $0 facebookresearch/coordinator prod"
    exit 1
fi

image_name=$1
image_tag=$2

# TODO: This relies on a very hacky setup: we check the other tags of the image
# given and look for the longest one... that one is probably a git commit. This
# is *super* sketchy but currently we have no way internally of looking up the
# same github commit as our internal commit at build-time, so... this is the
# best we have. It *really* needs improved once we have that available since
# this is one hell of an assumption to make.
res=$(docker image inspect "$image_name:$image_tag" \
    | jq '.[0].RepoTags' \
    | awk '{gsub(/"/, "", $1); print length,$1}' \
    | sort -n -s \
    | tail -n -1 \
    | cut -d" " -f2-)

echo "$res"
