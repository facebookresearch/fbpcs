/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <algorithm>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <map>
#include <memory>
#include <string>

#include <re2/re2.h>
#include <utility>

#include <fbpcf/common/FunctionalUtil.h>
#include <fbpcf/io/api/FileIOWrappers.h>
#include <vector>
#include "fbpcs/emp_games/common/Csv.h"
#include "fbpcs/emp_games/common/PrivateData.h"
#include "folly/json.h"
#include "folly/logging/xlog.h"

#include "fbpcs/emp_games/attribution/decoupled_aggregation/AggregationMetrics.h"
#include "fbpcs/emp_games/attribution/decoupled_aggregation/AttributionResult.h"
#include "fbpcs/emp_games/attribution/decoupled_aggregation/Constants.h"
#include "fbpcs/emp_games/attribution/decoupled_aggregation/ConversionMetadata.h"
#include "fbpcs/emp_games/attribution/decoupled_aggregation/TouchPointMetadata.h"

namespace aggregation::private_aggregation {

static const std::vector<TouchpointMetadata> parseTouchpointMetadata(
    const int lineNo,
    const std::vector<std::string>& header,
    const std::vector<std::string>& parts) {
  std::vector<int64_t> originalAdIds;
  std::vector<int64_t> timestamps;
  std::vector<int64_t> isClicks;
  std::vector<int64_t> campaignMetadata;
  for (std::vector<std::string>::size_type i = 0; i < header.size(); ++i) {
    auto column = header[i];
    auto value = parts[i];
    if (column == "ad_ids") {
      originalAdIds = getInnerArray<int64_t>(value);
    } else if (column == "timestamps") {
      timestamps = getInnerArray<int64_t>(value);
    } else if (column == "is_click") {
      isClicks = getInnerArray<int64_t>(value);
    } else if (column == "campaign_metadata") {
      campaignMetadata = getInnerArray<int64_t>(value);
    }
  }

  CHECK_EQ(originalAdIds.size(), timestamps.size())
      << "Ad ids and timestamps arrays are not the same length.";
  CHECK_EQ(originalAdIds.size(), isClicks.size())
      << "Ad ids and is_click arrays are not the same length.";
  CHECK_EQ(originalAdIds.size(), campaignMetadata.size())
      << "Ad ids and campaign_metadata arrays are not the same length.";

  std::vector<int64_t> unique_ids;
  for (std::vector<int64_t>::size_type i = 0; i < timestamps.size(); i++) {
    unique_ids.push_back(static_cast<int64_t>(i));
  }

  const std::unordered_set<int64_t> idSet{unique_ids.begin(), unique_ids.end()};
  CHECK_EQ(idSet.size(), timestamps.size())
      << "Found non-unique id for line " << lineNo << ". "
      << "This implementation currently only supports unique touchpoint ids per user.";

  std::vector<TouchpointMetadata> tpms;
  for (std::vector<int64_t>::size_type i = 0; i < originalAdIds.size(); i++) {
    tpms.push_back(TouchpointMetadata{
        /* original Ad Id */ originalAdIds.at(i),
        /* ts */ timestamps.at(i),
        /* isClick */ isClicks.at(i) == 1,
        /* campaignMetadata */ campaignMetadata.at(i),
        /* compressed Ad Id */ 0});
  }

  std::sort(tpms.begin(), tpms.end());

  return tpms;
}

static const std::vector<int64_t> retrieveOriginalAdIds(
    const std::vector<std::vector<TouchpointMetadata>>&
        touchpointMetadataArrays) {
  std::unordered_set<int64_t> adIdSet;
  for (auto& touchpointMetadataArray : touchpointMetadataArrays) {
    for (auto& touchpointMetadata : touchpointMetadataArray) {
      if (touchpointMetadata.originalAdId > 0) {
        adIdSet.insert(touchpointMetadata.originalAdId);
      }
    }
  }

  // Added a check here to make sure that number of ad Ids never exceed 65,536
  // (16 unsigned bit)
  CHECK_LE(adIdSet.size(), 65536)
      << "Number of ad Ids cannot be more than 65,536.";

  std::vector<int64_t> validOriginalAdIds;
  validOriginalAdIds.insert(
      validOriginalAdIds.end(), adIdSet.begin(), adIdSet.end());
  std::sort(validOriginalAdIds.begin(), validOriginalAdIds.end());
  return validOriginalAdIds;
}

// Ad Ids are represent by 64 bit integers. For measurement aggregation
// computation, the number of ad Ids received is much smaller. Thus for the
// computation, we are
// mapping original adId to compressed adId. This method will map the adIds to
// compressed adIds.
// replace all original ad Ids with compressed values in touchpoint Metadata and
// return the map of
// compressed ad Id to original ad Id.
static void replaceAdIdWithCompressedAdId(
    std::vector<std::vector<TouchpointMetadata>>& touchpointMetadataArrays,
    std::vector<int64_t>& validOriginalAdIds) {
  auto compressedAdId = 1;
  std::unordered_map<int64_t, uint16_t> adIdToCompressedAdIdMap;

  for (auto adId : validOriginalAdIds) {
    adIdToCompressedAdIdMap.insert({adId, compressedAdId});
    compressedAdId++;
  }

  for (auto& touchpointMetadataArray : touchpointMetadataArrays) {
    for (auto& touchpointMetadata : touchpointMetadataArray) {
      if (touchpointMetadata.originalAdId > 0) {
        touchpointMetadata.adId =
            adIdToCompressedAdIdMap.at(touchpointMetadata.originalAdId);
      }
    }
  }
}

// Aggregation Formats are received by publisher and will be shared to partner
// privately. We need to parse input data before that, so in this case we are
// extracting fields for all aggregators - currently measurement and PCM. During
// the game then, once aggregator formats are shared between both publisher and
// partner. We will then extract the fields required for only those aggregators.
static const std::vector<ConversionMetadata> parseConversions(
    const std::vector<std::string>& header,
    const std::vector<std::string>& parts) {
  std::vector<int64_t> convTimestamps;
  std::vector<int32_t> convValues;
  std::vector<int32_t> convMetadata;

  for (std::vector<std::string>::size_type i = 0; i < header.size(); ++i) {
    auto column = header[i];
    auto value = parts[i];

    if (column == "conversion_timestamps") {
      convTimestamps = getInnerArray<int64_t>(value);
    } else if (column == "conversion_values") {
      convValues = getInnerArray<int32_t>(value);
    } else if (column == "conversion_metadata") {
      convMetadata = getInnerArray<int32_t>(value);
    }
  }

  CHECK_EQ(convTimestamps.size(), convValues.size())
      << "Conversion timestamps and conversion value arrays are not the same length.";
  CHECK_EQ(convTimestamps.size(), convMetadata.size())
      << "Conversion timestamps and  arrays are not the same length.";

  std::vector<ConversionMetadata> convs;
  for (std::vector<int64_t>::size_type i = 0; i < convTimestamps.size(); i++) {
    convs.push_back(ConversionMetadata{
        /* ts */ convTimestamps.at(i),
        /* value */ convValues.at(i),
        /* metadata */ convMetadata.at(i)});
  }

  std::sort(convs.begin(), convs.end());
  return convs;
}

AggregationInputMetrics::AggregationInputMetrics(
    int myRole,
    std::filesystem::path inputSecretShareFilePath,
    std::filesystem::path inputClearTextFilePath,
    std::string aggregationFormatNamesStr) {
  XLOGF(
      INFO,
      "Reading attribution result file {}",
      inputSecretShareFilePath.string());
  XLOGF(
      INFO, "Reading metadata input file {}", inputClearTextFilePath.string());
  XLOGF(
      INFO, "Parsing input metadata file {}", inputClearTextFilePath.string());

  if (myRole == PUBLISHER) {
    auto aggregationFormatNames = private_measurement::csv::splitByComma(
        aggregationFormatNamesStr, false);
    CHECK_GT(aggregationFormatNames.size(), 0)
        << "No aggregation formats found";
    for (auto name : aggregationFormatNames) {
      aggregationFormats_.push_back(getAggregationFormatFromNameOrThrow(name));
    }
  }

  // Parse the input metadata file
  auto lineNo = 0;
  auto success = private_measurement::csv::readCsv(
      inputClearTextFilePath,
      [&](const std::vector<std::string>& header,
          const std::vector<std::string>& parts) {
        if (lineNo == 0) {
          XLOGF(DBG, "{}", private_measurement::vecToString(header));
        }
        XLOGF(DBG, "{}: {}", lineNo, private_measurement::vecToString(parts));

        ids_.push_back(lineNo);

        if (myRole == PUBLISHER) {
          touchpointMetadataArrays_.push_back(
              parseTouchpointMetadata(lineNo, header, parts));
        } else {
          conversiontMetadataArrays_.push_back(parseConversions(header, parts));
        }

        lineNo++;
      });

  if (!success) {
    XLOGF(
        FATAL,
        "Failed to read input metadata file {},",
        inputClearTextFilePath.string());
  }

  XLOGF(
      INFO,
      "Parsing input secret share file {}",
      inputSecretShareFilePath.string());
  // Reading the attribution results received from private attribution game in
  // an unordered_map.
  auto attributionResultJson = folly::parseJson(
      fbpcf::io::FileIOWrappers::readFile(inputSecretShareFilePath));

  for (const auto& [rule, formatters] : attributionResultJson.items()) {
    attributionRules_.push_back(rule.asString());
  }

  if (myRole == PUBLISHER) {
    touchpointSecretShare_ =
        AggregationMetrics::getAttributionsArrayfromDynamic(
            attributionResultJson);

    XLOG(INFO, "Replacing original ad Ids with compressed ad Ids");
    originalAdIds_ = retrieveOriginalAdIds(touchpointMetadataArrays_);
    replaceAdIdWithCompressedAdId(touchpointMetadataArrays_, originalAdIds_);
  } else {
    conversionSecretShare_ =
        AggregationMetrics::getAttributionsArrayfromDynamic(
            attributionResultJson);
  }
}

} // namespace aggregation::private_aggregation
