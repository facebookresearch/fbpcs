/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

// Temporary utility file to copy a local file to S3 until PCF supports it
#include "fbpcs/data_processing/common/S3FileWriter.h"

#include <filesystem>
#include <fstream>
#include <ios>
#include <memory>
#include <stdexcept>
#include <string>

#include <aws/s3/model/PutObjectRequest.h>

namespace private_lift::file_writer {

void S3FileWriter::write(
    const std::filesystem::path& src,
    const std::string& dest) {
  const auto& ref = fbpcf::aws::uriToObjectReference(dest);
  Aws::S3::Model::PutObjectRequest request;

  request.SetBucket(ref.bucket);
  request.SetKey(ref.key);

  auto fs = std::make_shared<std::fstream>(src, std::ios_base::in);
  fs->seekp(0, std::ios_base::end);
  request.SetBody(fs);
  request.SetContentLength(static_cast<size_t>(request.GetBody()->tellp()));
  auto outcome = s3Client_->PutObject(request);

  if (!outcome.IsSuccess()) {
    throw std::runtime_error{outcome.GetError().GetMessage()};
  }
}

} // namespace private_lift::file_writer
