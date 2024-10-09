/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <cstdint>
#include <memory>
#include <string>
#include <vector>
#include "fbpcs/pc_translator/input_processing/PCInstructionSet.h"

namespace pc_translator {

/*
 * This class contains functions required for PC Translator during actual run
 * i.e. retrieving the PC instruction sets, filtering the set per active GK for
 * run, encoding and decoding the dataset files input as per the instruction
 * set.
 */
class PCTranslator {
 public:
  explicit PCTranslator(const std::string& pcsFeatures)
      : pcsFeatures_(pcsFeatures) {}

  explicit PCTranslator(
      const std::string& pcsFeatures,
      const std::string& instructionSetBasePath)
      : pcsFeatures_(pcsFeatures),
        instructionSetBasePath_(instructionSetBasePath) {}

  std::string encode(const std::string& inputDataset);

  /*
   * Method to decode final aggregated output with the encoded breakdown Ids as
   * the keys. This method will decode the breakdown Ids to original group Id
   * values and format the aggregated output as per the new keys. Output of this
   * method would be the path of the decoded aggregated output.
   */
  std::string decode(const std::string& aggregatedOutputDataset);

 private:
  std::string pcsFeatures_;
  std::string instructionSetBasePath_ =
      "https://pc-translator.s3.us-west-2.amazonaws.com/";
  std::vector<std::shared_ptr<PCInstructionSet>> retrieveInstructionSets(
      std::vector<std::string>& instructionSetNames);
  std::vector<std::string> retrieveInstructionSetNamesForRun(
      const std::string& pcsFeatures);
  std::shared_ptr<PCInstructionSet> parseInstructionSet(
      std::string& instructionSet);
  std::string transformDataset(
      const std::string& inputData,
      std::shared_ptr<pc_translator::PCInstructionSet> pcInstructionSet);
};

} // namespace pc_translator
