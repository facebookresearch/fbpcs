/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <memory>
#include <string>

#include <aws/s3/model/PutObjectResult.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include "fbpcf/aws/AwsSdk.h"
#include "fbpcf/aws/MockS3Client.h"

#include "fbpcs/data_processing/common/FileWriterUtility.h"
#include "fbpcs/data_processing/common/LocalFileWriter.h"
#include "fbpcs/data_processing/common/S3FileWriter.h"

using ::testing::_;
using ::testing::Return;

const std::string s3Dest = "https://bucket.s3.region.amazonaws.com/key";
const std::string gcsDest = "https://storage.cloud.google.com/bucket/key";
const std::string localDest = "/dir/to/file";

TEST(FileWriterUtiltiyTest, getFileWriter) {
  fbpcf::AwsSdk::aquire();
  auto s3Writer = private_lift::file_writer::getFileWriter(s3Dest);
  auto& s3WriterRef = *s3Writer;
  EXPECT_EQ(
      typeid(s3WriterRef), typeid(private_lift::file_writer::S3FileWriter));

  auto localWriter = private_lift::file_writer::getFileWriter(localDest);
  auto& localWriterRef = *localWriter;
  EXPECT_EQ(
      typeid(localWriterRef),
      typeid(private_lift::file_writer::LocalFileWriter));
}

MATCHER_P2(
    requestWithBucketAndKey,
    bucket,
    key,
    "S3 request with expected parameteres") {
  return arg.GetBucket() == bucket && arg.GetKey() == key;
}

TEST(S3FileWriterTest, testWriteSuccess) {
  std::string runPath = __FILE__;
  std::string basePath = runPath.substr(0, runPath.rfind("/") + 1);
  auto filePath = "buffered_reader_example_file.txt";
  auto fullFilePath = basePath + filePath;

  fbpcf::AwsSdk::aquire();
  auto s3Client = std::make_unique<fbpcf::MockS3Client>();

  Aws::S3::Model::PutObjectResult result{};
  EXPECT_CALL(*s3Client, PutObject(requestWithBucketAndKey("bucket", "key")))
      .WillOnce(Return(Aws::S3::Model::PutObjectOutcome(result)));

  private_lift::file_writer::S3FileWriter s3Writer{std::move(s3Client)};
  s3Writer.write(fullFilePath, s3Dest);
}

TEST(S3FileWriterTest, testWriteException) {
  std::string runPath = __FILE__;
  std::string basePath = runPath.substr(0, runPath.rfind("/") + 1);
  auto filePath = "buffered_reader_example_file.txt";
  auto fullFilePath = basePath + filePath;

  fbpcf::AwsSdk::aquire();
  auto s3Client = std::make_unique<fbpcf::MockS3Client>();

  Aws::Client::AWSError<Aws::S3::S3Errors> error(
      Aws::S3::S3Errors::INTERNAL_FAILURE, false);
  EXPECT_CALL(*s3Client, PutObject(requestWithBucketAndKey("bucket", "key")))
      .WillOnce(Return(Aws::S3::Model::PutObjectOutcome(error)));

  private_lift::file_writer::S3FileWriter s3Writer{std::move(s3Client)};
  EXPECT_THROW(s3Writer.write(fullFilePath, s3Dest), std::runtime_error);
}

TEST(LocalFileWriterTest, testWriteSuccess) {
  std::string runPath = __FILE__;
  std::string basePath = runPath.substr(0, runPath.rfind("/") + 1);
  auto filePath = "buffered_reader_example_file.txt";
  auto fullFilePath = basePath + filePath;
  auto destPath = basePath + "dest_file.txt";
  std::cout << destPath << std::endl;

  private_lift::file_writer::LocalFileWriter localWriter;
  localWriter.write(fullFilePath, destPath);

  EXPECT_TRUE(std::filesystem::exists(destPath));
  std::filesystem::remove(destPath);
}
