/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "fbpcs/emp_games/lift/calculator/LiftInputData.h"

#include <string>
#include <vector>

#include <emp-tool/emp-tool.h>

#include <fbpcf/mpc/EmpGame.h>

#include "fbpcs/emp_games/lift/calculator/LiftDataFrameBuilder.h"
#include "fbpcs/emp_games/lift/common/Column.h"
#include "fbpcs/emp_games/lift/common/DataFrame.h"

static constexpr int64_t kConversionCap = 25;

using namespace private_lift;

LiftInputData::LiftInputData(fbpcf::Party party, const std::string& filePath)
    : LiftInputData{LiftDataFrameBuilder{filePath, kConversionCap}, party} {}

LiftInputData::LiftInputData(
    const LiftDataFrameBuilder& builder,
    fbpcf::Party party)
    : party_{party}, groupKey_{getGroupKeyForParty(party)} {
  // Will implement in next diff
}

int64_t LiftInputData::calculateGroupCount() const {
  int64_t maxId = 0;
  // Will implement in next diff
  return maxId;
}

std::vector<df::Column<emp::Bit>> LiftInputData::calculateBitmasks() const {
  std::vector<df::Column<emp::Bit>> res;
  // Will implement in next diff
  return res;
}
