/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "fbpcs/pc_translator/PCTranslator.h"

namespace pc_translator {

std::string PCTranslator::encode(const std::string& /* inputDataset */) {
  throw std::runtime_error("Unimplemented");
}

std::string PCTranslator::decode(
    const std::string& /* aggregatedOutputDataset */) {
  throw std::runtime_error("Unimplemented");
}

void PCTranslator::retrieveInstructionSets(
    std::vector<std::string>& /* instructionSetNames */) {
  throw std::runtime_error("Unimplemented");
}

std::vector<std::string> PCTranslator::retrieveInstructionSetNamesForRun(
    const std::string& /* pcsFeatures */) {
  throw std::runtime_error("Unimplemented");
}

void PCTranslator::transformDataset(const std::string& /* input */) {
  throw std::runtime_error("Unimplemented");
}

void PCTranslator::parseInstructionSet(
    const std::string& /* instructionSet */) {
  throw std::runtime_error("Unimplemented");
}
} // namespace pc_translator
