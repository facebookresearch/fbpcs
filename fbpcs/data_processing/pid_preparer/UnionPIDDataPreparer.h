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

/*
 * This chunk size has to be large enough that we don't make
 * unnecessary trips to cloud storage but small enough that
 * we don't cause OOM issues. This chunk size was chosen based
 * on the size of our containers as well as the expected size
 * of our files to fit the aforementioned constraints.
 */
constexpr size_t kBufferedReaderChunkSize = 1073741824; // 2^30

class UnionPIDDataPreparer {
 public:
  UnionPIDDataPreparer(
      const std::string& inputPath,
      const std::string& outputPath,
      const std::filesystem::path& tmpDirectory,
      int64_t maxColumnCnt = 1,
      int64_t logEveryN = 1'000)
      : inputPath_{inputPath},
        outputPath_{outputPath},
        tmpDirectory_{tmpDirectory},
        logEveryN_{logEveryN},
        maxColumnCnt_{maxColumnCnt} {}

  UnionPIDDataPreparerResults prepare() const;

 private:
  /* Split a string on the given delimiter */
  std::vector<std::string> split(std::string& str, const std::string& delim)
      const;

  std::string inputPath_;
  std::string outputPath_;
  std::filesystem::path tmpDirectory_;
  int64_t logEveryN_;
  int64_t maxColumnCnt_;
};

} // namespace measurement::pid
