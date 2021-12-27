/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <string>

namespace private_measurement::test_util {

// Get the basedir from the file path
std::string getBaseDirFromPath(const std::string& filePath);

} // namespace private_measurement::test_util
