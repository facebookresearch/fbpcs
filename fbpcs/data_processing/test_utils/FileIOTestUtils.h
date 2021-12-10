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
void writeVecToFile(
    const std::vector<std::string>& rows,
    const std::string& filePath);

/**
 * Check that the data at the given filepath matches the vector of rows.
 *
 * @param filePath the filepath to read contents against `rows`
 * @param rows the vector of rows to compare against the file
 * @notes internally calls EXPECT_EQ from gtest
 */
void expectFileRowsEqual(
    const std::string& filePath,
    std::vector<std::string>& rows);
} // namespace data_processing::test_utils
