#! /bin/bash
# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

# Upload the binaries:
./upload-binaries-to-s3-test.sh emp_games brian_test
./upload-binaries-to-s3-test.sh data_processing brian_test
./upload-binaries-to-s3-test.sh pid brian_test
./upload-binaries-to-s3-test.sh validation brian_test
./upload_scripts/upload-binaries-using-onedocker.sh emp_games brian_test -c "upload_scripts/configs/test_upload_binaries_config.yml"
./upload_scripts/upload-binaries-using-onedocker.sh data_processing brian_test -c "upload_scripts/configs/test_upload_binaries_config.yml"
./upload_scripts/upload-binaries-using-onedocker.sh pid brian_test -c "upload_scripts/configs/test_upload_binaries_config.yml"
./upload_scripts/upload-binaries-using-onedocker.sh validation brian_test -c "upload_scripts/configs/test_upload_binaries_config.yml"


od_upload=$(aws s3 ls --recursive s3://one-docker-repository-test-psi | awk '{ print $4, $3}' | sort)

aws_upload=$(aws s3 ls --recursive s3://one-docker-repository-custom-1 | awk '{ print $4, $3}' | sort)

echo "Diff Results:"
diff -yb <(echo "$od_upload") <(echo "$aws_upload")
echo "-----Finished------"
