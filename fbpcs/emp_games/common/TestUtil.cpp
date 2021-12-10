/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "./TestUtil.h"

namespace private_measurement::test_util {

std::string getBaseDirFromPath(const std::string& filePath) {
  return filePath.substr(0, filePath.rfind("/") + 1);
}

} // namespace private_measurement::test_util
