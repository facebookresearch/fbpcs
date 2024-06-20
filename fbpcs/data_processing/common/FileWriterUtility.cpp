/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "fbpcs/data_processing/common/FileWriterUtility.h"

#include <filesystem>
#include <memory>
#include <stdexcept>
#include <string>

#include "fbpcf/io/FileManagerUtil.h"

#include "fbpcs/data_processing/common/IFileWriter.h"
#include "fbpcs/data_processing/common/LocalFileWriter.h"
#include "fbpcs/data_processing/common/S3FileWriter.h"

namespace private_lift::file_writer {

void write(const std::filesystem::path& src, const std::string& dest) {
  auto writer = getFileWriter(src);
  return writer->write(src, dest);
}

std::unique_ptr<IFileWriter> getFileWriter(const std::string& fileName) {
  auto outputType = fbpcf::io::getFileType(fileName);
  switch (outputType) {
    case fbpcf::io::FileType::S3:
      return std::make_unique<S3FileWriter>();
    case fbpcf::io::FileType::Local:
      return std::make_unique<LocalFileWriter>();
    default:
      throw std::runtime_error{"Unsupported output destination"};
  }
}
} // namespace private_lift::file_writer
