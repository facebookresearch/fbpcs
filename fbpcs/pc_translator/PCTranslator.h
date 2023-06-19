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
      : pcsfeatures_(pcsFeatures) {}

  /*
   * Method to encode the configurable fields in input dataset as per the active
   * pc instruction sets for the run. This method will output the path of
   * transformed input dataset, which can be used in further PC run.
   */
  std::string encode(const std::string& inputDataset);

  /*
   * Method to decode final aggregated output with the encoded breakdown Ids as
   * the keys. This method will decode the breakdown Ids to original group Id
   * values and format the aggregated output as per the new keys. Output of this
   * method would be the path of the decoded aggregated output.
   */
  std::string decode(const std::string& aggregatedOutputDataset);

 private:
  std::string pcsfeatures_;
  void retrieveInstructionSets(std::vector<std::string>& instructionSetNames);
  std::vector<std::string> retrieveInstructionSetNamesForRun(
      const std::string& pcsfeatures);
  void parseInstructionSet(const std::string& instructionSet);
  void transformDataset(const std::string& input);
};

} // namespace pc_translator
