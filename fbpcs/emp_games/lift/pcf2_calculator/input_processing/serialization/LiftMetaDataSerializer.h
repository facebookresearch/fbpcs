/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <optional>
#include <vector>

#include "fbpcs/emp_games/common/IMetadataSerializer.h"
#include "fbpcs/emp_games/lift/pcf2_calculator/input_processing/InputData.h"

namespace private_lift {

class LiftMetaDataSerializer : common::IMetadataSerializer {
 public:
  explicit LiftMetaDataSerializer(
      InputData inputData,
      int32_t numConversionsPerUser,
      std::optional<std::vector<int32_t>> reverseUnionMap = std::nullopt,
      std::optional<size_t> unionSize = std::nullopt)
      : inputData_{inputData},
        numConversionsPerUser_(numConversionsPerUser),
        reverseUnionMap_(reverseUnionMap),
        unionSize_(unionSize) {}

  std::vector<std::vector<unsigned char>> serializePublisherMetadata() override;

  std::vector<std::vector<unsigned char>> serializePartnerMetadata() override;

 private:
  InputData inputData_;
  int32_t numConversionsPerUser_;
  std::optional<std::vector<int32_t>> reverseUnionMap_;
  std::optional<size_t> unionSize_;
};

} // namespace private_lift
