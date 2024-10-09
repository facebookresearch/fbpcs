/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <folly/json.h>
#include <cstdint>
#include <memory>
#include <string>
#include <vector>
#include "fbpcs/pc_translator/input_processing/FilterConstraint.h"

namespace pc_translator {

/*
 * Class to store PC Instruction set. This class contains a list of group Ids as
 * well as list of filter constraints.
 */
class PCInstructionSet {
 public:
  /*
   * Method to all group Ids from the PC instruction set.
   */
  const std::vector<std::string>& getGroupByIds() const;

  /*
   * Method to get all filter constraints from PC instruction set.
   */
  const std::vector<FilterConstraint>& getFilterConstraints() const;

  /*
   * Method to get parse and create PCInstructionSet instance.
   */
  static PCInstructionSet fromDynamic(const folly::dynamic& obj);

 private:
  std::vector<std::string> groupByIds;
  std::vector<FilterConstraint> filterConstraints;

  void parseJson(const std::string& json);
};

} // namespace pc_translator
