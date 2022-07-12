/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

// Temporary utility file to copy local files
#include "fbpcs/data_processing/common/LocalFileWriter.h"

#include <filesystem>
#include <string>

namespace private_lift::file_writer {

void LocalFileWriter::write(
    const std::filesystem::path& src,
    const std::string& dest) {
  std::filesystem::path destPath{dest};

  if (destPath.has_parent_path()) {
    std::filesystem::create_directories(destPath.parent_path());
  }

  std::filesystem::copy(
      src, destPath, std::filesystem::copy_options::overwrite_existing);
}

} // namespace private_lift::file_writer
