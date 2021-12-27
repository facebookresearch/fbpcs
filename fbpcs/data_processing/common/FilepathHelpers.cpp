/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "FilepathHelpers.h"

#include <string>

namespace private_lift::filepath_helpers {

std::string getBaseFilename(const std::string& filename) {
  auto pos = filename.find_last_of('/');
  return filename.substr(pos + 1);
}

} // namespace private_lift::filepath_helpers
