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
#include <set>
#include "folly/String.h"

namespace pc_translator {

std::string PCTranslator::encode(const std::string& inputDataset) {
  auto validInstructionSetNames =
      PCTranslator::retrieveInstructionSetNamesForRun(pcsFeatures_);
  auto pcInstructionSets =
      PCTranslator::retrieveInstructionSets(validInstructionSetNames);
  PCTranslator::transformDataset(inputDataset, pcInstructionSets);
  return "";
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
    auto file_path = instructionSetBasePath + instructionSetName + ".json";
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
      [](const std::string& feature) { return feature.find("pc_instr") == 0; });

  return validPCInstructionSets;
}

void PCTranslator::transformDataset(
    const std::string& /* inputData */,
    const std::vector<std::shared_ptr<pc_translator::PCInstructionSet>>&
    /* pcInstructionSets */) {
  throw std::runtime_error("Unimplemented");
}

std::shared_ptr<PCInstructionSet> PCTranslator::parseInstructionSet(
    std::string& instructionSet) {
  return std::make_shared<PCInstructionSet>(PCInstructionSet::fromDynamic(
      folly::parseJson(std::move(instructionSet))));
}
} // namespace pc_translator
