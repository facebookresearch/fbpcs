/*
 * Copyright (c) Facebook, Inc. and its affiliates.
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
      : dataPath_{dataPath}, spinePath_{spinePath}, outputPath_{outputPath} , tmpDirectory_{tmpDirectory} {}

  void combineFile();

 private:
  std::filesystem::path dataPath_;
  std::filesystem::path spinePath_;
  std::filesystem::path outputPath_;
  std::filesystem::path tmpDirectory_;
};
} // namespace pid
