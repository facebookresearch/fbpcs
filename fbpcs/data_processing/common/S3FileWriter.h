/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <filesystem>
#include <memory>
#include <string>

#include "fbpcf/aws/S3Util.h"

#include "fbpcs/data_processing/common/IFileWriter.h"

namespace private_lift::file_writer {

class S3FileWriter : public IFileWriter {
 public:
  S3FileWriter()
      : s3Client_(fbpcf::aws::createS3Client(fbpcf::aws::S3ClientOption{})) {}

  explicit S3FileWriter(std::unique_ptr<Aws::S3::S3Client> client)
      : s3Client_{std::move(client)} {}

  void write(const std::filesystem::path& src, const std::string& dest)
      override;

 private:
  std::unique_ptr<Aws::S3::S3Client> s3Client_;
};
} // namespace private_lift::file_writer
