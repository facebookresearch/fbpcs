/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <string>
#include <vector>

namespace data_processing::test_utils {
/**
 * Write a vector of rows to the given filepath.
 *
 * @param rows a vector of lines to be written to file
 * @param filePath the filepath where the rows should be written
 */
void writeVecToFile(const std::vector<std::string> &rows,
                    const std::string &filePath);
} // namespace data_processing::test_utils
