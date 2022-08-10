/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "fbpcs/data_processing/attribution_id_combiner/PidAttributionIdCombiner.h"

#include <folly/String.h>
#include <folly/logging/xlog.h>
#include <re2/re2.h>
#include <iomanip>
#include <istream>
#include <stdexcept>
#include <unordered_map>

#include "fbpcs/data_processing/id_combiner/IdSwapMultiKey.h"

namespace pid::combiner {
PidAttributionIdCombiner::PidAttributionIdCombiner()
    : spineIdFilePath(FLAGS_spine_path), outputPath{FLAGS_output_path} {
  XLOG(INFO) << "Starting attribution id combiner run on: data_path:"
             << FLAGS_data_path << ", spine_path: " << FLAGS_spine_path
             << ", output_path: " << FLAGS_output_path
             << ", tmp_directory: " << FLAGS_tmp_directory
             << ", sorting_strategy: " << FLAGS_sort_strategy
             << ", max_id_column_cnt: " << FLAGS_max_id_column_cnt
             << ", protocol_type: " << FLAGS_protocol_type;

  auto dataReader = std::make_unique<fbpcf::io::FileReader>(FLAGS_data_path);
  auto spineReader = std::make_unique<fbpcf::io::FileReader>(FLAGS_spine_path);
  dataFile = std::make_shared<fbpcf::io::BufferedReader>(
      std::move(dataReader), kBufferedReaderChunkSize);
  spineIdFile = std::make_shared<fbpcf::io::BufferedReader>(
      std::move(spineReader), kBufferedReaderChunkSize);
}
PidAttributionIdCombiner::~PidAttributionIdCombiner() {
  dataFile->close();
  spineIdFile->close();
}

std::stringstream PidAttributionIdCombiner::idSwap(std::string headerLine) {
  std::stringstream idSwapOutFile;
  idSwapMultiKey(
      dataFile,
      spineIdFile,
      idSwapOutFile,
      FLAGS_max_id_column_cnt,
      headerLine,
      spineIdFilePath);
  return idSwapOutFile;
}

void PidAttributionIdCombiner::run() {
  auto meta = processHeader(dataFile);
  std::stringstream idSwapOutFile = idSwap(meta.headerLine);
  aggregate(idSwapOutFile, meta, outputPath);
}

} // namespace pid::combiner
