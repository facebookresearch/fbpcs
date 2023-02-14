/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "fbpcs/emp_games/common/TestUtil.h"

namespace private_measurement::test_util {

std::string getBaseDirFromPath(const std::string& filePath) {
  auto dir = filePath.substr(0, filePath.rfind("/") + 1);
  std::string toErase = "fbcode/";
  size_t pos = dir.find(toErase);
  if (pos != std::string::npos) {
    // If found then erase it from string
    return dir.substr(pos + toErase.size());
  }
  return dir;
}

} // namespace private_measurement::test_util
