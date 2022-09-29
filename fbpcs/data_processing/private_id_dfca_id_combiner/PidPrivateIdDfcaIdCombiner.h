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
#include "fbpcf/io/api/FileReader.h"
#include "fbpcs/data_processing/private_id_dfca_id_combiner/PrivateIdDfcaStrategy.h"

namespace pid::combiner {
/*
PidPrivateIdDfcaIdCombiner is a child class that inherited
PrivateIdDfcaStrategy. It takes the data file and the spine id file (output from
pid match step) and prepare the format for the compute stage.

It assumes that the publisher columns are:
id_, user_id_publisher

and that the partner columns are:
id_, user_id_partner


For example:
If the input data file was:
id_, user_id_publisher
1       a1
2       a2

And the input spine id file was:
id_, private_id
1    AAA
2    BBB


Then the output would be:
id_,     user_id_partner
AAA               a1
BBB               a2
*/
class PidPrivateIdDfcaIdCombiner : public PrivateIdDfcaStrategy {
  std::shared_ptr<fbpcf::io::BufferedReader> dataFile;
  std::shared_ptr<fbpcf::io::BufferedReader> spineIdFile;
  std::string spineIdFilePath;
  std::filesystem::path outputPath;

 public:
  explicit PidPrivateIdDfcaIdCombiner();
  std::stringstream idSwap(std::string headerLine);
  void run() override;
  virtual ~PidPrivateIdDfcaIdCombiner() override;
};

} // namespace pid::combiner
