/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <gtest/gtest.h>
#include <filesystem>
#include <fstream>
#include <functional>
#include <utility>
#include <vector>

#include "fbpcs/emp_games/common/Util.h"
#include "fbpcs/emp_games/lift/pcf2_calculator/input_processing/serialization/LiftMetaDataSerializer.h"
#include "fbpcs/emp_games/lift/pcf2_calculator/input_processing/test/TestUtil.h"
#include "fbpcs/emp_games/lift/pcf2_calculator/sample_input/SampleInput.h"

using namespace ::testing;

namespace private_lift {

class LiftMetaDataSerializerTest : public ::testing::Test {
 protected:
  int numConversionsPerUser_;
  void SetUp() override {
    std::string tempDir = std::filesystem::temp_directory_path();

    auto partnerInputFile = private_lift::sample_input::getPartnerInput2();

    numConversionsPerUser_ = 2;
    bool computePublisherBreakdowns_ = true;
  }
};

TEST_F(LiftMetaDataSerializerTest, testSerializePublisherMetadata) {
  auto publisherInputFile = private_lift::sample_input::getPublisherInput3();

  int epoch = 1546300800;

  auto publisherInputData = InputData(
      publisherInputFile.native(),
      InputData::LiftMPCType::Standard,
      true /*computePublisherBreakdowns_*/,
      epoch,
      numConversionsPerUser_);

  auto publisherSerializer =
      LiftMetaDataSerializer(publisherInputData, numConversionsPerUser_);

  std::vector<std::vector<unsigned char>> result =
      publisherSerializer.serializePublisherMetadata();

  std::vector<std::vector<unsigned char>> expected = {
      {1, 0, 0, 0, 0},    {0, 0, 0, 0, 0},    {3, 0, 0, 0, 0},
      {0, 100, 0, 0, 0},  {4, 100, 0, 0, 0},  {6, 100, 0, 0, 0},
      {0, 100, 0, 0, 0},  {5, 100, 0, 0, 0},  {6, 100, 0, 0, 0},
      {0, 100, 0, 0, 0},  {5, 100, 0, 0, 0},  {6, 100, 0, 0, 0},
      {1, 100, 0, 0, 0},  {4, 100, 0, 0, 0},  {6, 100, 0, 0, 0},
      {1, 100, 0, 0, 0},  {5, 100, 0, 0, 0},  {6, 100, 0, 0, 0},
      {0, 100, 0, 0, 0},  {4, 100, 0, 0, 0},  {6, 100, 0, 0, 0},
      {1, 100, 0, 0, 0},  {5, 100, 0, 0, 0},  {6, 100, 0, 0, 0},
      {9, 0, 0, 0, 0},    {12, 100, 0, 0, 0}, {12, 100, 0, 0, 0},
      {13, 100, 0, 0, 0}, {13, 100, 0, 0, 0}, {13, 100, 0, 0, 0},
      {1, 100, 0, 0, 0},  {4, 100, 0, 0, 0},  {7, 100, 0, 0, 0}};
  EXPECT_EQ(result, expected);
}

TEST_F(LiftMetaDataSerializerTest, testSerializePartnerMetadata) {
  auto partnerInputFile = private_lift::sample_input::getPartnerInput2();

  int epoch = 1546300800;

  auto partnerInputData = InputData(
      partnerInputFile.native(),
      InputData::LiftMPCType::Standard,
      true /*computePublisherBreakdowns_*/,
      epoch,
      numConversionsPerUser_);

  auto partnerSerializer =
      LiftMetaDataSerializer(partnerInputData, numConversionsPerUser_);

  std::vector<std::vector<unsigned char>> result =
      partnerSerializer.serializePartnerMetadata();

  std::vector<std::vector<unsigned char>> expected = {
      {1, 0, 0,   0, 0, 0, 0,   0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
       0, 0, 100, 0, 0, 0, 110, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
      {1, 1, 0,   0, 0, 0, 0,   0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
       0, 0, 100, 0, 0, 0, 110, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
      {1, 0, 0,   0, 0, 0, 0,   0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
       0, 0, 100, 0, 0, 0, 110, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
      {1, 0, 0,   0,  0, 0, 0, 0,   0, 0, 0,  0, 0, 0, 0,
       0, 0, 144, 1,  0, 0, 0, 0,   0, 0, 50, 0, 0, 0, 60,
       0, 0, 0,   20, 0, 0, 0, 144, 1, 0, 0,  0, 0, 0, 0},
      {1, 0, 0,   0,  0, 0, 0, 0,   0, 0, 0,  0, 0, 0, 0,
       0, 0, 144, 1,  0, 0, 0, 0,   0, 0, 50, 0, 0, 0, 60,
       0, 0, 0,   20, 0, 0, 0, 144, 1, 0, 0,  0, 0, 0, 0},
      {1, 1, 0,   0,  0, 0, 0, 0,   0, 0, 0,  0, 0, 0, 0,
       0, 0, 144, 1,  0, 0, 0, 0,   0, 0, 50, 0, 0, 0, 60,
       0, 0, 0,   20, 0, 0, 0, 144, 1, 0, 0,  0, 0, 0, 0},
      {1, 1, 0,   0,  0, 0, 0, 0,   0, 0, 0,   0, 0, 0, 0,
       0, 0, 144, 1,  0, 0, 0, 0,   0, 0, 100, 0, 0, 0, 110,
       0, 0, 0,   20, 0, 0, 0, 144, 1, 0, 0,   0, 0, 0, 0},
      {1, 1, 0,   0,  0, 0, 0, 0,   0, 0, 0,   0, 0, 0, 0,
       0, 0, 144, 1,  0, 0, 0, 0,   0, 0, 100, 0, 0, 0, 110,
       0, 0, 0,   20, 0, 0, 0, 144, 1, 0, 0,   0, 0, 0, 0},
      {1, 0, 0,   0,  0, 0, 0, 0,   0, 0, 0,   0, 0, 0, 0,
       0, 0, 144, 1,  0, 0, 0, 0,   0, 0, 100, 0, 0, 0, 110,
       0, 0, 0,   20, 0, 0, 0, 144, 1, 0, 0,   0, 0, 0, 0},
      {1, 1, 0,   0,  0, 0, 0, 0,   0, 0, 0,  0, 0, 0, 0,
       0, 0, 144, 1,  0, 0, 0, 0,   0, 0, 90, 0, 0, 0, 100,
       0, 0, 0,   20, 0, 0, 0, 144, 1, 0, 0,  0, 0, 0, 0},
      {1, 1, 0,   0,  0, 0, 0, 0,   0, 0, 0,  0, 0, 0, 0,
       0, 0, 144, 1,  0, 0, 0, 0,   0, 0, 90, 0, 0, 0, 100,
       0, 0, 0,   20, 0, 0, 0, 144, 1, 0, 0,  0, 0, 0, 0},
      {1, 0, 0,   0,  0, 0, 0, 0,   0, 0, 0,  0, 0, 0, 0,
       0, 0, 144, 1,  0, 0, 0, 0,   0, 0, 90, 0, 0, 0, 100,
       0, 0, 0,   20, 0, 0, 0, 144, 1, 0, 0,  0, 0, 0, 0},
      {1, 0, 0,   0,  0, 150, 0, 0,   0, 160, 0,   0, 0, 10, 0,
       0, 0, 132, 3,  0, 0,   0, 0,   0, 0,   200, 0, 0, 0,  210,
       0, 0, 0,   20, 0, 0,   0, 144, 1, 0,   0,   0, 0, 0,  0},
      {1, 1, 0,   0,  0, 150, 0, 0,   0, 160, 0,   0, 0, 10, 0,
       0, 0, 132, 3,  0, 0,   0, 0,   0, 0,   200, 0, 0, 0,  210,
       0, 0, 0,   20, 0, 0,   0, 144, 1, 0,   0,   0, 0, 0,  0},
      {1, 1, 0,   0,  0, 150, 0, 0,   0, 160, 0,   0, 0, 10, 0,
       0, 0, 132, 3,  0, 0,   0, 0,   0, 0,   200, 0, 0, 0,  210,
       0, 0, 0,   20, 0, 0,   0, 144, 1, 0,   0,   0, 0, 0,  0},
      {1, 0, 0,   0,  0, 50, 0, 0,   0, 60, 0,   0, 0, 10, 0,
       0, 0, 132, 3,  0, 0,  0, 0,   0, 0,  150, 0, 0, 0,  160,
       0, 0, 0,   20, 0, 0,  0, 144, 1, 0,  0,   0, 0, 0,  0},
      {1, 0, 0,   0,  0, 50, 0, 0,   0, 60, 0,   0, 0, 10, 0,
       0, 0, 132, 3,  0, 0,  0, 0,   0, 0,  150, 0, 0, 0,  160,
       0, 0, 0,   20, 0, 0,  0, 144, 1, 0,  0,   0, 0, 0,  0},
      {1, 0, 0,   0,  0, 50, 0, 0,   0, 60, 0,   0, 0, 10, 0,
       0, 0, 132, 3,  0, 0,  0, 0,   0, 0,  150, 0, 0, 0,  160,
       0, 0, 0,   20, 0, 0,  0, 144, 1, 0,  0,   0, 0, 0,  0},
      {1, 0, 0,   0,  0, 30, 0, 0,   0, 40, 0,  0, 0, 10, 0,
       0, 0, 132, 3,  0, 0,  0, 0,   0, 0,  50, 0, 0, 0,  60,
       0, 0, 0,   20, 0, 0,  0, 144, 1, 0,  0,  0, 0, 0,  0},
      {1, 0, 0,   0,  0, 30, 0, 0,   0, 40, 0,  0, 0, 10, 0,
       0, 0, 132, 3,  0, 0,  0, 0,   0, 0,  50, 0, 0, 0,  60,
       0, 0, 0,   20, 0, 0,  0, 144, 1, 0,  0,  0, 0, 0,  0},
      {1, 0, 0,   0,  0, 30, 0, 0,   0, 40, 0,  0, 0, 10, 0,
       0, 0, 132, 3,  0, 0,  0, 0,   0, 0,  50, 0, 0, 0,  60,
       0, 0, 0,   20, 0, 0,  0, 144, 1, 0,  0,  0, 0, 0,  0},
      {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
       0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
      {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
       0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
      {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
       0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
      {1, 0, 0,   0,  0, 0, 0, 0,   0, 0, 0,   0, 0, 0, 0,
       0, 0, 196, 9,  0, 0, 0, 0,   0, 0, 100, 0, 0, 0, 110,
       0, 0, 0,   50, 0, 0, 0, 196, 9, 0, 0,   0, 0, 0, 0},
      {1, 0, 0,   0,  0, 0, 0, 0,   0, 0, 0,  0, 0, 0, 0,
       0, 0, 196, 9,  0, 0, 0, 0,   0, 0, 50, 0, 0, 0, 60,
       0, 0, 0,   50, 0, 0, 0, 196, 9, 0, 0,  0, 0, 0, 0},
      {1, 2, 0,   0,  0, 0, 0, 0,   0, 0, 0,   0, 0, 0, 0,
       0, 0, 196, 9,  0, 0, 0, 0,   0, 0, 150, 0, 0, 0, 160,
       0, 0, 0,   50, 0, 0, 0, 196, 9, 0, 0,   0, 0, 0, 0},
      {1, 2, 0,   0,  0, 150, 0, 0,   0, 160, 0,   0, 0, 10, 0,
       0, 0, 132, 3,  0, 0,   0, 0,   0, 0,   200, 0, 0, 0,  210,
       0, 0, 0,   20, 0, 0,   0, 144, 1, 0,   0,   0, 0, 0,  0},
      {1, 0, 0,   0,  0, 50, 0, 0,   0, 60, 0,   0, 0, 10, 0,
       0, 0, 132, 3,  0, 0,  0, 0,   0, 0,  150, 0, 0, 0,  160,
       0, 0, 0,   20, 0, 0,  0, 144, 1, 0,  0,   0, 0, 0,  0},
      {1, 0, 0,   0,  0, 30, 0, 0,   0, 40, 0,  0, 0, 10, 0,
       0, 0, 132, 3,  0, 0,  0, 0,   0, 0,  50, 0, 0, 0,  60,
       0, 0, 0,   20, 0, 0,  0, 144, 1, 0,  0,  0, 0, 0,  0},
      {1, 2, 0,   0,   0,   0,   0,   0,   0, 0, 0,   0, 0, 0, 0,
       0, 0, 196, 9,   0,   0,   0,   0,   0, 0, 200, 0, 0, 0, 210,
       0, 0, 0,   206, 255, 255, 255, 196, 9, 0, 0,   0, 0, 0, 0},
      {1, 2, 0,   0,   0,   0,   0,   0,   0, 0, 0,   0, 0, 0, 0,
       0, 0, 196, 9,   0,   0,   0,   0,   0, 0, 200, 0, 0, 0, 210,
       0, 0, 0,   206, 255, 255, 255, 196, 9, 0, 0,   0, 0, 0, 0},
      {1, 2, 0,   0,   0,   0,   0,   0,   0, 0, 0,   0, 0, 0, 0,
       0, 0, 196, 9,   0,   0,   0,   0,   0, 0, 200, 0, 0, 0, 210,
       0, 0, 0,   206, 255, 255, 255, 196, 9, 0, 0,   0, 0, 0, 0}};
  EXPECT_EQ(result, expected);
}

} // namespace private_lift
