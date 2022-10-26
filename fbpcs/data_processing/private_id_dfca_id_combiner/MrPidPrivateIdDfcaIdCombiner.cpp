/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "fbpcs/data_processing/private_id_dfca_id_combiner/MrPidPrivateIdDfcaIdCombiner.h"

#include <boost/algorithm/string.hpp>
#include <folly/Random.h>
#include <folly/String.h>
#include <folly/logging/xlog.h>
#include <re2/re2.h>
#include <iomanip>
#include <istream>
#include <stdexcept>
#include <unordered_map>

#include "fbpcs/data_processing/id_combiner/IdSwapMultiKey.h"

namespace pid::combiner {
MrPidPrivateIdDfcaIdCombiner::MrPidPrivateIdDfcaIdCombiner()
    : spineIdFilePath(FLAGS_spine_path), outputPath{FLAGS_output_path} {
  XLOG(INFO) << "Starting private_id_dfca id combiner run on: "
             << "spine_path: " << FLAGS_spine_path
             << ", output_path: " << FLAGS_output_path
             << ", tmp_directory: " << FLAGS_tmp_directory
             << ", sorting_strategy: " << FLAGS_sort_strategy
             << ", max_id_column_cnt: " << FLAGS_max_id_column_cnt
             << ", protocol_type: " << FLAGS_protocol_type;

  auto spineReader = std::make_unique<fbpcf::io::FileReader>(FLAGS_spine_path);
  spineIdFile =
      std::make_shared<fbpcf::io::BufferedReader>(std::move(spineReader));
}
MrPidPrivateIdDfcaIdCombiner::~MrPidPrivateIdDfcaIdCombiner() {
  spineIdFile->close();
}

std::stringstream MrPidPrivateIdDfcaIdCombiner::idSwap(std::string headerLine) {
  std::stringstream idSwapOutFile;
  idSwapOutFile << headerLine << "\n";
  while (!spineIdFile->eof()) {
    auto spineRow = spineIdFile->readLine();
    idSwapOutFile << spineRow << "\n";
  }
  return idSwapOutFile;
}

void MrPidPrivateIdDfcaIdCombiner::run() {
  auto meta = processHeader(spineIdFile);
  std::stringstream idSwapOutFile = idSwap(meta.headerLine);
  aggregate(idSwapOutFile, outputPath);
}

} // namespace pid::combiner
