# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict


class AwsException(Exception):
    pass


class AwsCloudwatchException(AwsException):
    pass


class AwsS3Exception(AwsException):
    pass


class AwsKinesisException(AwsException):
    pass


class AwsCloudwatchLogsFetchException(AwsCloudwatchException):
    pass


class AwsS3FolderCreationException(AwsS3Exception):
    pass


class AwsS3FolderContentFetchException(AwsS3Exception):
    pass


class AwsS3UploadFailedException(AwsS3Exception):
    pass


class AwsCloudwatchLogGroupFetchException(AwsCloudwatchException):
    pass


class AwsCloudwatchLogStreamFetchException(AwsCloudwatchException):
    pass


class AwsS3BucketVerificationException(AwsS3Exception):
    pass


class AwsKinesisFirehoseDeliveryStreamFetchException(AwsKinesisException):
    pass
