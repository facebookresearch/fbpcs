/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <filesystem>
#include <string>
#include <unordered_map>
#include <vector>

#include "fbpcs/emp_games/common/Csv.h"
#include "fbpcs/emp_games/he_aggregation/AttributionAdditiveSSResult.h"
#include "fbpcs/emp_games/pcf2_aggregation/TouchpointMetadata.h"

namespace pcf2_he {

/*
 * This class represents input data for Private Aggregation.
 * It processes an input csv and generates the std::vectors for each column
 */
class AggregationInputMetrics {
 public:
  explicit AggregationInputMetrics(
      common::InputEncryption inputEncryption,
      std::filesystem::path inputSecretShareFilePath,
      std::filesystem::path inputClearTextFilePaths);

  explicit AggregationInputMetrics(
      std::vector<int64_t> ids,
      std::vector<std::vector<std::vector<AttributionAdditiveSSResult>>>
          attributionSecretShare,
      std::vector<std::vector<pcf2_aggregation::TouchpointMetadata>>
          touchpointMetadataArrays)
      : ids_{ids},
        attributionSecretShare_{attributionSecretShare},
        touchpointMetadataArrays_{touchpointMetadataArrays} {}

  const std::vector<int64_t>& getIds() const {
    return ids_;
  }

  const std::vector<std::vector<std::vector<AttributionAdditiveSSResult>>>
  getAttributionSecretShares() const {
    return attributionSecretShare_;
  }

  const std::vector<std::vector<pcf2_aggregation::TouchpointMetadata>>&
  getTouchpointMetadata() const {
    return touchpointMetadataArrays_;
  }

 private:
  std::vector<int64_t> ids_;
  std::vector<std::vector<std::vector<AttributionAdditiveSSResult>>>
      attributionSecretShare_;
  std::vector<std::vector<pcf2_aggregation::TouchpointMetadata>>
      touchpointMetadataArrays_;
};

} // namespace pcf2_he
