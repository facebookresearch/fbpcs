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
#include "fbpcs/data_processing/private_id_dfca_id_combiner/PrivateIdDfcaStrategy.h"

namespace pid::combiner {
/*
MrPrivateIdDfcaIdCombiner is a child class that inherited
PrivateIdDfcaStrategy. It takes the data file and the spine id file (output from
pid match step) and prepare the format for the compute stage.

It assumes that the publisher columns are:
id_, publisher_user_id

and that the partner columns are:
id_, partner_user_id

For example:
If the input data file was:
id_, publisher_user_id
1       a1
2       a2

And the input spine id file was:
id_, private_id
1    AAA
2    BBB

Then the output would be:
id_,     partner_user_id
AAA               a1
BBB               a2
*/
class MrPidPrivateIdDfcaIdCombiner : public PrivateIdDfcaStrategy {
  std::shared_ptr<fbpcf::io::BufferedReader> spineIdFile;
  std::string spineIdFilePath;
  std::filesystem::path outputPath;
  std::filesystem::path tmpFilepath;

 public:
  explicit MrPidPrivateIdDfcaIdCombiner();
  virtual ~MrPidPrivateIdDfcaIdCombiner() override;
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
   * 3. aggregate the spine file according to private_id_dfca format.
   **/
  void run() override;
};

} // namespace pid::combiner
