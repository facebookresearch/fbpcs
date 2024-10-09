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
#include <cstdint>
#include <iterator>
#include <set>
#include <stdexcept>
#include <string>
#include "fbpcs/emp_games/common/Csv.h"
#include "folly/String.h"

namespace pc_translator {

std::string PCTranslator::encode(const std::string& inputDatasetPath) {
  auto validInstructionSetNames =
      PCTranslator::retrieveInstructionSetNamesForRun(pcsFeatures_);
  auto pcInstructionSets =
      PCTranslator::retrieveInstructionSets(validInstructionSetNames);
  if (pcInstructionSets.empty()) {
    // No instruction set found. return the input dataset path.
    return inputDatasetPath;
  }
  return PCTranslator::transformDataset(
      inputDatasetPath, pcInstructionSets.front());
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
    const std::string& inputDatasetPath,
    std::shared_ptr<pc_translator::PCInstructionSet> pcInstructionSet) {
  // Parse the input CSV
  auto lineNo = 0;
  std::vector<std::vector<uint32_t>> inputColums;
  std::vector<std::string> outputHeader;
  std::vector<std::vector<std::string>> outputContent;
  private_measurement::csv::readCsv(
      inputDatasetPath,
      [&](const std::vector<std::string>& header,
          const std::vector<std::string>& parts) {
        std::vector<uint32_t> inputColumnPerRow;
        std::string column;
        std::uint32_t value;
        bool found = false;
        std::vector<std::string> outputContentPerRow;
        for (std::vector<std::string>::size_type i = 0; i < header.size();
             ++i) {
          column = header[i];
          value = std::atoi(parts[i].c_str());
          found =
              (std::find(
                   pcInstructionSet->getGroupByIds().begin(),
                   pcInstructionSet->getGroupByIds().end(),
                   column) != pcInstructionSet->getGroupByIds().end());
          if (found) {
            inputColumnPerRow.push_back(value);
          } else {
            if (lineNo == 0) {
              outputHeader.push_back(header[i]);
            }
            outputContentPerRow.push_back(parts[i]);
          }
        }

        inputColums.push_back(inputColumnPerRow);
        outputContent.push_back(outputContentPerRow);
        lineNo++;
      });

  auto filters = std::make_unique<
      std::vector<std::unique_ptr<fbpcf::mpc_std_lib::oram::IFilter>>>(0);
  std::unique_ptr<fbpcf::mpc_std_lib::oram::IOramEncoder> encoder =
      std::make_unique<fbpcf::mpc_std_lib::oram::OramEncoder>(
          std::move(filters));

  auto encodedIndexes = encoder->generateORAMIndexes(inputColums);

  auto dir = inputDatasetPath.substr(0, inputDatasetPath.rfind("/") + 1);
  auto output_dataset_path = dir + "transformed_publisher_input.csv";

  PCTranslator::putOutputData(
      output_dataset_path, outputHeader, outputContent, encodedIndexes);
  return output_dataset_path;
}

void PCTranslator::putOutputData(
    const std::string& output_dataset_path,
    std::vector<std::string>& outputHeader,
    std::vector<std::vector<std::string>>& outputContent,
    const std::vector<uint32_t>& encodedIndexes) {
  outputHeader.push_back("breakdown_id");

  if (outputContent.size() != encodedIndexes.size()) {
    throw std::runtime_error(
        "Encoded index vector size should match the input vector size.");
  }

  for (std::vector<std::string>::size_type i = 0; i < encodedIndexes.size();
       ++i) {
    auto indexVec = std::to_string(encodedIndexes[i]);
    outputContent[i].push_back(indexVec);
  }

  private_measurement::csv::writeCsv(
      output_dataset_path, outputHeader, outputContent);
}

std::shared_ptr<PCInstructionSet> PCTranslator::parseInstructionSet(
    std::string& instructionSet) {
  return std::make_shared<PCInstructionSet>(PCInstructionSet::fromDynamic(
      folly::parseJson(std::move(instructionSet))));
}
} // namespace pc_translator
