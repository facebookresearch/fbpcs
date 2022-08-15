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

#include "fbpcf/io/api/BufferedReader.h"
#include "fbpcf/io/api/FileIOWrappers.h"
#include "fbpcf/io/api/FileReader.h"
#include "fbpcs/data_processing/common/FilepathHelpers.h"
#include "fbpcs/data_processing/lift_id_combiner/LiftStrategy.h"
namespace pid::combiner {
class MrPidLiftIdCombiner : public LiftStrategy {
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
  explicit MrPidLiftIdCombiner(
      std::string spineIdFilePath,
      std::string outputStr,
      std::string tmpDirectory,
      std::string sortStrategy,
      int maxIdColumnCnt,
      std::string protocolType);
  /**
   * 1. If data is from publisher, aggregate the data for same private id
   *according to the rule of dataswarm
   * 2. If data is from partner, just output the data from spine data fine line
   *by line
   * @param meta meta data which has headerline and isPublisherDataset to run
   *idSwapMultiKey()
   * @return stringstream output stream of mr pid matching result
   **/
  std::stringstream idSwap(FileMetaData meta);
  /**
   * run() has three steps
   * 1. process header, get file type and other meta data
   * 2. get pid output intermediate file. If the data is from publisher,
   *aggregate before next step. Otherwise data is same as spine data file
   * 3. aggregate the spine file according to lift format.
   **/
  void run() override;
  virtual ~MrPidLiftIdCombiner() override;
};

} // namespace pid::combiner
