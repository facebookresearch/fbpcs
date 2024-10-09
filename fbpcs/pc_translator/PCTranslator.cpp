/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "fbpcs/pc_translator/PCTranslator.h"
#include "fbpcs/pc_translator/input_processing/PCInstructionSet.h"

#include <fbpcf/common/FunctionalUtil.h>
#include <fbpcf/io/api/FileIOWrappers.h>
#include <fbpcf/mpc_std_lib/oram/encoder/IFilter.h>
#include <fbpcf/mpc_std_lib/oram/encoder/IOramEncoder.h>
#include <fbpcf/mpc_std_lib/oram/encoder/OramEncoder.h>
#include <algorithm>
#include <set>
#include <stdexcept>
#include "fbpcs/emp_games/common/Csv.h"
#include "folly/String.h"

namespace pc_translator {

std::string PCTranslator::encode(const std::string& inputDataset) {
  auto validInstructionSetNames =
      PCTranslator::retrieveInstructionSetNamesForRun(pcsFeatures_);
  auto pcInstructionSets =
      PCTranslator::retrieveInstructionSets(validInstructionSetNames);
  if (pcInstructionSets.empty()) {
    // No instruction set found. return the input dataset path.
    return inputDataset;
  }
  return PCTranslator::transformDataset(
      inputDataset, pcInstructionSets.front());
}

std::string PCTranslator::decode(
    const std::string& /* aggregatedOutputDataset */) {
  throw std::runtime_error("Unimplemented");
}

std::vector<std::shared_ptr<PCInstructionSet>>
PCTranslator::retrieveInstructionSets(
    std::vector<std::string>& instructionSetNames) {
  std::vector<std::shared_ptr<PCInstructionSet>> pcInstructionSets;
  for (auto instructionSetName : instructionSetNames) {
    instructionSetName.erase(
        remove(instructionSetName.begin(), instructionSetName.end(), '\''),
        instructionSetName.end());
    instructionSetName.erase(
        remove(instructionSetName.begin(), instructionSetName.end(), ' '),
        instructionSetName.end());
    auto file_path = instructionSetBasePath_ + instructionSetName + ".json";
    auto contents = fbpcf::io::FileIOWrappers::readFile(file_path);
    pcInstructionSets.push_back(PCTranslator::parseInstructionSet(contents));
  }
  return pcInstructionSets;
}

std::vector<std::string> PCTranslator::retrieveInstructionSetNamesForRun(
    const std::string& pcsFeatures) {
  std::set<std::string> enabledFeatureFlags;
  folly::splitTo<std::string>(
      ',',
      pcsFeatures,
      std::inserter(enabledFeatureFlags, enabledFeatureFlags.begin()),
      true);

  std::vector<std::string> validPCInstructionSets;
  std::copy_if(
      enabledFeatureFlags.begin(),
      enabledFeatureFlags.end(),
      std::back_inserter(validPCInstructionSets),
      [](const std::string& feature) {
        return feature.find("pc_instr") != std::string::npos;
      });

  return validPCInstructionSets;
}

std::string PCTranslator::transformDataset(
    const std::string& inputData,
    std::shared_ptr<pc_translator::PCInstructionSet> pcInstructionSet) {
  // Parse the input CSV
  auto lineNo = 0;
  std::vector<std::vector<uint32_t>> inputColums;
  private_measurement::csv::readCsv(
      inputData,
      [&](const std::vector<std::string>& header,
          const std::vector<std::string>& parts) {
        std::vector<uint32_t> inputColumnPerRow;
        for (std::vector<std::string>::size_type i = 0; i < header.size();
             ++i) {
          auto& column = header[i];
          auto value = std::atoi(parts[i].c_str());
          auto iter = std::find(
              pcInstructionSet->getGroupByIds().begin(),
              pcInstructionSet->getGroupByIds().end(),
              column);
          if (iter != pcInstructionSet->getGroupByIds().end()) {
            inputColumnPerRow.push_back(value);
          }
        }

        inputColums.push_back(inputColumnPerRow);
        lineNo++;
      });

  auto filters = std::make_unique<
      std::vector<std::unique_ptr<fbpcf::mpc_std_lib::oram::IFilter>>>(0);
  std::unique_ptr<fbpcf::mpc_std_lib::oram::IOramEncoder> encoder =
      std::make_unique<fbpcf::mpc_std_lib::oram::OramEncoder>(
          std::move(filters));

  auto encodedIndexes = encoder->generateORAMIndexes(inputColums);

  // TODO : Append the enodedIndexes at the end of publisher output and return
  // output path.
  return "";
}

std::shared_ptr<PCInstructionSet> PCTranslator::parseInstructionSet(
    std::string& instructionSet) {
  return std::make_shared<PCInstructionSet>(PCInstructionSet::fromDynamic(
      folly::parseJson(std::move(instructionSet))));
}
} // namespace pc_translator
