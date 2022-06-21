/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <filesystem>
#include <memory>
#include <string>
#include <thread>
#include <unordered_map>

#include <gtest/gtest.h>

#include <fbpcf/io/FileManagerUtil.h>
#include "fbpcs/emp_games/common/TestUtil.h"
#include "fbpcs/emp_games/lift/pcf2_calculator/test/common/LiftCalculator.h"

namespace private_lift {

TEST(LiftCalculatorLocalTest, JsonCorrectnessTest) {
  uint64_t epoch = 1546300800;
  uint64_t numCohorts = 3;
  uint64_t numPublisherBreakdowns = 2;
  std::string baseDir =
      private_measurement::test_util::getBaseDirFromPath(__FILE__);
  std::string publisherInputPath =
      baseDir + "../../../sample_input/publisher_unittest3.csv";
  std::string partnerInputPath =
      baseDir + "../../../sample_input/partner_2_convs_unittest.csv";
  std::string expectedOutputPath =
      baseDir + "../../../sample_input/correctness_output.json";

  LiftCalculator liftCalculator(numCohorts, numPublisherBreakdowns, epoch);
  std::ifstream inFilePublisher{publisherInputPath};
  std::ifstream inFilePartner{partnerInputPath};
  int32_t tsOffset = 10;
  std::string linePublisher;
  std::string linePartner;
  getline(inFilePublisher, linePublisher);
  getline(inFilePartner, linePartner);
  auto headerPublisher =
      private_measurement::csv::splitByComma(linePublisher, false);
  auto headerPartner =
      private_measurement::csv::splitByComma(linePartner, false);
  std::unordered_map<std::string, int> colNameToIndex =
      liftCalculator.mapColToIndex(headerPublisher, headerPartner);
  GroupedLiftMetrics result = liftCalculator.compute(
      inFilePublisher, inFilePartner, colNameToIndex, tsOffset, false);
  GroupedLiftMetrics expectedResult =
      GroupedLiftMetrics::fromJson(fbpcf::io::read(expectedOutputPath));

  EXPECT_EQ(result, expectedResult);
}

} // namespace private_lift
