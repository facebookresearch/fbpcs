/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <filesystem>
#include <string>
#include <vector>

namespace measurement::pid {

struct UnionPIDDataPreparerResults {
  int64_t linesProcessed = 0;
  int64_t duplicateIdCount = 0;
};

class UnionPIDDataPreparer {
 public:
  UnionPIDDataPreparer(
      const std::string& inputPath,
      const std::string& outputPath,
      const std::filesystem::path& tmpDirectory,
      int64_t logEveryN = 1'000)
      : inputPath_{inputPath},
        outputPath_{outputPath},
        tmpDirectory_{tmpDirectory},
        logEveryN_{logEveryN} {}

  UnionPIDDataPreparerResults prepare() const;

 private:
  /* Split a string on the given delimiter */
  std::vector<std::string> split(std::string& str, const std::string& delim)
      const;

  std::string inputPath_;
  std::string outputPath_;
  std::filesystem::path tmpDirectory_;
  int64_t logEveryN_;
};

} // namespace measurement::pid
