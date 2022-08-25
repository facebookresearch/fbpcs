/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "fbpcs/data_processing/lift_id_combiner/PidLiftIdCombiner.h"

#include <boost/algorithm/string.hpp>
#include <folly/String.h>
#include <folly/logging/xlog.h>
#include <re2/re2.h>
#include <iomanip>
#include <istream>
#include <ostream>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <vector>

#include "fbpcf/io/api/BufferedReader.h"
#include "fbpcf/io/api/FileReader.h"
#include "fbpcs/data_processing/common/FilepathHelpers.h"
#include "fbpcs/data_processing/id_combiner/DataPreparationHelpers.h"
#include "fbpcs/data_processing/id_combiner/IdSwapMultiKey.h"
#include "fbpcs/data_processing/lift_id_combiner/LiftIdSpineCombinerOptions.h"

namespace pid::combiner {
PidLiftIdCombiner::PidLiftIdCombiner(
    std::string dataPath,
    std::string spineIdFilePath,
    std::string outputStr,
    std::string tmpDirectory,
    std::string sortStrategy,
    int maxIdColumnCnt,
    std::string protocolType)
    : spineIdFilePath(spineIdFilePath),
      tmpDirectory(tmpDirectory),
      sortStrategy(sortStrategy),
      maxIdColumnCnt(maxIdColumnCnt),
      outputPath{outputStr} {
  XLOG(INFO) << "Starting attribution id combiner run on: data_path:"
             << dataPath << ", spine_path: " << spineIdFilePath
             << ", output_path: " << outputPath
             << ", tmp_directory: " << tmpDirectory
             << ", sorting_strategy: " << sortStrategy
             << ", max_id_column_cnt: " << maxIdColumnCnt
             << ", protocol_type: " << protocolType;

  auto dataReader = std::make_unique<fbpcf::io::FileReader>(dataPath);
  auto spineReader = std::make_unique<fbpcf::io::FileReader>(spineIdFilePath);
  dataFile = std::make_shared<fbpcf::io::BufferedReader>(
      std::move(dataReader), kBufferedReaderChunkSize);
  spineIdFile = std::make_shared<fbpcf::io::BufferedReader>(
      std::move(spineReader), kBufferedReaderChunkSize);
}
PidLiftIdCombiner::~PidLiftIdCombiner() {
  dataFile->close();
  spineIdFile->close();
}

std::stringstream PidLiftIdCombiner::idSwap(FileMetaData meta) {
  std::stringstream idSwapOutFile;
  if (meta.isPublisherDataset) {
    pid::combiner::idSwapMultiKey(
        dataFile,
        spineIdFile,
        idSwapOutFile,
        maxIdColumnCnt,
        meta.headerLine,
        spineIdFilePath,
        true);
  } else {
    pid::combiner::idSwapMultiKey(
        dataFile,
        spineIdFile,
        idSwapOutFile,
        maxIdColumnCnt,
        meta.headerLine,
        spineIdFilePath);
  }

  return idSwapOutFile;
}

void PidLiftIdCombiner::run() {
  auto meta = processHeader(dataFile);
  std::stringstream idSwapOutFile = idSwap(meta);
  aggregate(
      idSwapOutFile,
      meta.isPublisherDataset,
      outputPath,
      tmpDirectory,
      sortStrategy);
}

} // namespace pid::combiner
