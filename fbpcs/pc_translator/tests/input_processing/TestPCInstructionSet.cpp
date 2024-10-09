/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <folly/json.h>
#include <filesystem>
#include <string>

#include <fbpcf/io/api/FileIOWrappers.h>
#include <gtest/gtest.h>
#include "../../../emp_games/common/TestUtil.h"
#include "fbpcs/pc_translator/input_processing/PCInstructionSet.h"

namespace pc_translator {

using IFilter = fbpcf::mpc_std_lib::oram::IFilter;

class TestPCInstructionSet : public ::testing::Test {
 public:
 protected:
  std::string testInstructionSetPath_;

  void SetUp() override {
    std::string baseDir =
        private_measurement::test_util::getBaseDirFromPath(__FILE__);
    testInstructionSetPath_ = baseDir + "pc_instr_test_instruction_set.json";
  }
};

TEST_F(TestPCInstructionSet, TestStandardWorkflowTest) {
  auto pcInstructionSet = std::make_shared<PCInstructionSet>(
      PCInstructionSet::fromDynamic(folly::parseJson(
          fbpcf::io::FileIOWrappers::readFile(testInstructionSetPath_))));
  auto groupByIds = pcInstructionSet->getGroupByIds();
  auto filterConstraints = pcInstructionSet->getFilterConstraints();
  EXPECT_EQ(groupByIds.size(), 2);
  EXPECT_EQ(filterConstraints.size(), 3);
  EXPECT_EQ(filterConstraints[0].getName(), "gender");
  EXPECT_EQ(filterConstraints[0].getType(), IFilter::FilterType::EQ);
  EXPECT_EQ(filterConstraints[0].getValue(), 0);
}

} // namespace pc_translator
