/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <string>

namespace private_lift::hash_slinging_salter {

std::string saltedHash(const std::string& id, const std::string& key);
std::string base64SaltedHashFromBase64Key(
    const std::string& id,
    const std::string& base64_key);

} // namespace private_lift::hash_slinging_salter
