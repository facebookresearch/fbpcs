/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <gtest/gtest.h>
#include "../../emp_games/common/TestUtil.h"
#include "fbpcs/pc_translator/PCTranslator.h"

namespace pc_translator {
class TestPCTranslator : public ::testing::Test {
 public:
 protected:
  std::string pcs_features_;
  std::string test_instruction_set_base_path_;
  std::string test_publisher_input_path_;

  void SetUp() override {
    pcs_features_ =
        "'num_mpc_container_mutation', 'private_lift_unified_data_process', 'pc_instr_test_instruction_set'";
    std::string baseDir =
        private_measurement::test_util::getBaseDirFromPath(__FILE__);
    test_instruction_set_base_path_ = baseDir + "input_processing/";
    test_publisher_input_path_ = baseDir + "publisher_unittest.csv";
  }
};

TEST_F(TestPCTranslator, TestEncode) {
  auto pcTranslator = std::make_shared<PCTranslator>(
      pcs_features_, test_instruction_set_base_path_);
  auto outputPath = pcTranslator->encode(test_publisher_input_path_);
  EXPECT_EQ(outputPath, "");
}
} // namespace pc_translator
