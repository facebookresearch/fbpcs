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

#include "fbpcs/data_processing/common/IFileWriter.h"

namespace private_lift::file_writer {

void write(const std::filesystem::path& src, const std::string& dest);

std::unique_ptr<IFileWriter> getFileWriter(const std::string& fileName);

} // namespace private_lift::file_writer
