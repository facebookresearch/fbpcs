# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict


class AwsDeploymentError(Exception):
    pass


class AccessDeniedError(AwsDeploymentError):
    pass


class S3BucketCreationError(AwsDeploymentError):
    pass


class S3BucketVersioningFailedError(AwsDeploymentError):
    pass


class S3BucketDeleteError(AwsDeploymentError):
    pass
