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

static const int numConversionsPerUser = 2;

TEST(LiftMetaDataSerializerTest, testSerializePublisherMetadata) {
  auto publisherInputFile = private_lift::sample_input::getPublisherInput3();

  int epoch = 1546300800;

  auto publisherInputData = InputData(
      publisherInputFile.native(),
      InputData::LiftMPCType::Standard,
      true /*computePublisherBreakdowns_*/,
      epoch,
      numConversionsPerUser);

  auto publisherSerializer =
      LiftMetaDataSerializer(publisherInputData, numConversionsPerUser);

  EXPECT_NO_THROW(publisherSerializer.serializePartnerMetadata());
}

TEST(LiftMetaDataSerializerTest, testSerializePartnerMetadata) {
  auto partnerInputFile = private_lift::sample_input::getPartnerInput2();

  int epoch = 1546300800;

  auto partnerInputData = InputData(
      partnerInputFile.native(),
      InputData::LiftMPCType::Standard,
      true /*computePublisherBreakdowns_*/,
      epoch,
      numConversionsPerUser);

  auto partnerSerializer =
      LiftMetaDataSerializer(partnerInputData, numConversionsPerUser);

  EXPECT_NO_THROW(partnerSerializer.serializePartnerMetadata());
}

} // namespace private_lift
