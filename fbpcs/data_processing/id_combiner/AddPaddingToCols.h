/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <istream>
#include <ostream>
#include <string>
#include <unordered_map>
#include <vector>

namespace pid::combiner {
/*
This class implements the AddPaddingToCols that is used to
add padding to columns that contain lists. This is commonly used in
private measrurement computations to maintain privacy and avoid
leakage during MPC step
*/
void addPaddingToCols(
    std::istream& dataFilePath,
    const std::vector<std::string>& cols,
    const std::vector<int32_t>& padSizePerCol,
    bool enforceMax,
    std::ostream& outFilePath);
} // namespace pid::combiner
