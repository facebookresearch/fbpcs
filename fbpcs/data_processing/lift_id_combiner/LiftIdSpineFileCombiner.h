/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <filesystem>
#include <unordered_map>

#include "LiftIdSpineMultiConversionInput.h"

namespace pid {
/*
 * This chunk size has to be large enough that we don't make
 * unnecessary trips to cloud storage but small enough that
 * we don't cause OOM issues. This chunk size was chosen based
 * on the size of our containers as well as the expected size
 * of our files to fit the aforementioned constraints.
 */
constexpr size_t kBufferedReaderChunkSize = 1073741824; // 2^30
/*
This class implements the combiner that is used to combine the output of pid
partner and publisher files with the help of an identity spine from union pid
*/
class LiftIdSpineFileCombiner {
 public:
  LiftIdSpineFileCombiner(
      std::filesystem::path dataPath,
      std::filesystem::path spinePath,
      std::filesystem::path outputPath,
      std::filesystem::path tmpDirectory)
      : dataPath_{dataPath},
        spinePath_{spinePath},
        outputPath_{outputPath},
        tmpDirectory_{tmpDirectory} {}

  void combineFile();

 private:
  std::filesystem::path dataPath_;
  std::filesystem::path spinePath_;
  std::filesystem::path outputPath_;
  std::filesystem::path tmpDirectory_;
};
} // namespace pid
