/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <filesystem>
#include <fstream>
#include <iomanip>
#include <istream>
#include <ostream>
#include <unordered_map>
#include <vector>

#include "fbpcf/io/api/FileReader.h"
#include "fbpcs/data_processing/common/FilepathHelpers.h"
#include "fbpcs/data_processing/lift_id_combiner/LiftStrategy.h"
namespace pid::combiner {
/*
This class implements the combiner that is used to combine the output of pid
partner and publisher files with the help of an identity spine from union pid
*/
class PidLiftIdCombiner : public LiftStrategy {
  std::shared_ptr<fbpcf::io::BufferedReader> dataFile;
  std::shared_ptr<fbpcf::io::BufferedReader> spineIdFile;
  std::string spineIdFilePath;
  std::string tmpDirectory;
  std::string outputStr;
  std::string sortStrategy;
  std::string protocolType;
  int maxIdColumnCnt;
  std::filesystem::path outputPath;
  std::filesystem::path tmpFilepath;

 public:
  explicit PidLiftIdCombiner(
      std::string dataPath,
      std::string spineIdFilePath,
      std::string outputPath,
      std::string tmpDirectory,
      std::string sortStrategy,
      int maxIdColumnCnt,
      std::string protocolType);
  /**
   * idSwap() will call the idSwapMultipleKey() to get the pid output
   * intermediate file which combines union ids with the original data for
   * aggregate step.
   *
   * @param meta meta data which has headerline and isPublisherDataset to run
   *idSwapMultiKey()
   * @return stringstream output stream of mr pid matching result
   **/
  std::stringstream idSwap(FileMetaData meta);
  /**
   * run() has three steps
   * 1. process header, get file type and other meta data
   * 2. get pid output intermediate file which combines union ids with the
   * original data
   * 3. aggregate the spine file according to lift format.
   **/
  void run() override;
  virtual ~PidLiftIdCombiner() override;
};

} // namespace pid::combiner
