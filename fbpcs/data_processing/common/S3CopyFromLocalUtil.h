/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <filesystem>
#include <string>

namespace private_lift::s3_utils {

void uploadToS3(const std::filesystem::path& src, const std::string& dest);

} // namespace private_lift::s3_utils
