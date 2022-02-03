/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "fbpcs/data_processing/test_utils/FileIOTestUtils.h"

#include <algorithm>
#include <fstream>
#include <iterator>
#include <string>
#include <vector>

#include <gtest/gtest.h>

namespace data_processing::test_utils {
void writeVecToFile(
    const std::vector<std::string>& rows,
    const std::string& filePath) {
  std::ofstream oFile{filePath};
  std::ostream_iterator<std::string> outputIterator{oFile, "\n"};
  std::copy(rows.begin(), rows.end(), outputIterator);
}

void expectFileRowsEqual(
    const std::string& filePath,
    std::vector<std::string>& rows) {
  std::ifstream iFile{filePath};
  std::string line;
  std::size_t idx = 0;
  while (std::getline(iFile, line)) {
    EXPECT_EQ(line, rows.at(idx));
    ++idx;
  }
  // Ensure the file wasn't too *short*
  EXPECT_EQ(idx, rows.size());
}
} // namespace data_processing::test_utils
