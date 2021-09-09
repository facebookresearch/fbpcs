#!/bin/bash
# Copyright (c) Facebook, Inc. and its affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

set -e

IMAGE_NAME="pce-deployment"
TAG="latest"

echo "Building docker image..."

# Dockerfile cannot access parent directory so we have to bring the scripts here temporarily.
cp -r ../pce/aws_terraform_template .

docker build -t "$IMAGE_NAME:$TAG" .

echo "Cleaning up..."
rm -r aws_terraform_template

echo "Done"
