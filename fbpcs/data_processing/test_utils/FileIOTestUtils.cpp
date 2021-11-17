/*
 * Copyright (c) Facebook, Inc. and its affiliates.
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

namespace data_processing::test_utils {
void writeVecToFile(const std::vector<std::string> &rows,
                    const std::string &filePath) {
  std::ofstream oFile{filePath};
  std::ostream_iterator<std::string> outputIterator{oFile, "\n"};
  std::copy(rows.begin(), rows.end(), outputIterator);
}
} // namespace data_processing::test_utils
