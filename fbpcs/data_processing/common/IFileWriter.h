/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <filesystem>
#include <string>

namespace private_lift::file_writer {

class IFileWriter {
 public:
  virtual ~IFileWriter() {}

  virtual void write(
      const std::filesystem::path& src,
      const std::string& dest) = 0;
};

} // namespace private_lift::file_writer
