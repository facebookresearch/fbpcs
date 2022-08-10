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
#include "fbpcs/data_processing/attribution_id_combiner/AttributionStrategy.h"
#include "fbpcs/data_processing/common/FilepathHelpers.h"

namespace pid::combiner {
/*
MrPidAttributionIdCombiner is a child class that inherited AttributionStrategy.
It takes the spine id file (output from mr pid match step) and
prepare the format for the compute stage.

It assumes that the publisher columns are:
id_, ad_id, timestamp, is_click

and that the partner columns are:
id_, conversion_timestamp, conversion_value

It then leverages the helpers in id_combiner to group by the id_ column
and aggregate the remaining columns, padding them to all be exactly of length 4.

For example:
If the mr pid spine file was:
id_, ad_id, timestamp, is_click
AAA      a1        t1        1
AAA      a2        t2        0
BBB      a1        t1        0


Then the output would be:
id_,     ad_id,           timestamp,         is_click
AAA     [0, 0, a1, a2]    [0, 0, t1, t2]     [0, 0, 1, 0]
BBB     [0, 0, 0, a1]     [0, 0, 0, t1]      [0, 0, 0, 0]
*/
class MrPidAttributionIdCombiner : public AttributionStrategy {
  std::shared_ptr<fbpcf::io::BufferedReader> spineIdFile;
  std::string spineIdFilePath;
  std::filesystem::path outputPath;
  std::filesystem::path tmpFilepath;

 public:
  explicit MrPidAttributionIdCombiner();
  virtual ~MrPidAttributionIdCombiner() override;
  /**
   * idSwap() will change the spine file reader into a stringstream as the input
   * for aggregate step.
   *
   * @param headerLine header line
   * @return stringstream output stream of mr pid matching result
   **/
  std::stringstream idSwap(std::string headerLine);
  /**
   * run() has three steps
   * 1. process header, get file type and other meta data
   * 2. get the string stream of spine file
   * 3. aggregate the spine file according to attribution format.
   **/
  void run() override;
};

} // namespace pid::combiner
