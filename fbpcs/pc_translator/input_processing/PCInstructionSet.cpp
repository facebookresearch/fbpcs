/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "fbpcs/pc_translator/input_processing/PCInstructionSet.h"

#include <folly/json.h>
#include <cstdint>
#include <memory>
#include <string>
#include <vector>

namespace pc_translator {

const std::vector<std::string>& PCInstructionSet::getGroupByIds() const {
  return groupByIds;
}

const std::vector<FilterConstraint>& PCInstructionSet::getFilterConstraints()
    const {
  return filterConstraints;
}

PCInstructionSet PCInstructionSet::fromDynamic(const folly::dynamic& obj) {
  PCInstructionSet pcInstructionSet;
  auto aggregationConfig = obj["aggregated_metrics"];
  auto groupByFields = aggregationConfig["group_by"];

  for (auto groupByField : groupByFields) {
    pcInstructionSet.groupByIds.push_back(groupByField.asString());
  }

  auto filterConstraintsFields = aggregationConfig["filter"];

  for (auto& [key, constraints] : filterConstraintsFields.items()) {
    std::string name = key.asString();
    for (auto constraint : constraints) {
      auto constraintType = constraint["constraint_type"].asString();
      auto constraintValue = constraint["value"].asInt();
      FilterConstraint filterConstraint(name, constraintType, constraintValue);
      pcInstructionSet.filterConstraints.push_back(filterConstraint);
    }
  }

  return pcInstructionSet;
}

} // namespace pc_translator
