/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
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
This file implements the DataValidation that is used to
verify that all values in an input file valid. Specifically, it
will skip the headers row and will verify that the
remaining rows contain integers and that they match the length of the header
*/

void validateCsvData(std::istream& dataFile);

bool verifyHeaderContainsCols(
    std::vector<std::string> header,
    std::vector<std::string> cols);
} // namespace pid::combiner
