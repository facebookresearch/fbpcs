/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <string>

namespace private_lift::base64 {

std::string encode(const std::string& input);
std::string decode(const std::string& input);

} // namespace private_lift::base64
