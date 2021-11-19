/*
 * Copyright (c) Facebook, Inc. and its affiliates.
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
#include <fbpcf/io/FileManagerUtil.h>
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
  std::vector<int64_t> adIds;
  std::vector<int64_t> timestamps;
  std::vector<int64_t> isClicks;
  std::vector<int64_t> campaignMetadata;
  for (std::vector<std::string>::size_type i = 0; i < header.size(); ++i) {
    auto column = header[i];
    auto value = parts[i];
    if (column == "ad_ids") {
      adIds = getInnerArray<int64_t>(value);
    } else if (column == "timestamps") {
      timestamps = getInnerArray<int64_t>(value);
    } else if (column == "is_click") {
      isClicks = getInnerArray<int64_t>(value);
    } else if (column == "campaign_metadata") {
      campaignMetadata = getInnerArray<int64_t>(value);
    }
  }

  CHECK_EQ(adIds.size(), timestamps.size())
      << "Ad ids and timestamps arrays are not the same length.";
  CHECK_EQ(adIds.size(), isClicks.size())
      << "Ad ids and is_click arrays are not the same length.";
  CHECK_EQ(adIds.size(), campaignMetadata.size())
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
  for (std::vector<int64_t>::size_type i = 0; i < adIds.size(); i++) {
    tpms.push_back(TouchpointMetadata{
        /* adId */ adIds.at(i),
        /* ts */ timestamps.at(i),
        /* isClick */ isClicks.at(i) == 1,
        /* campaignMetadata */ campaignMetadata.at(i)});
  }

  std::sort(tpms.begin(), tpms.end());

  return tpms;
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
      << "Conversion timetamps and conversion value arrays are not the same length.";
  CHECK_EQ(convTimestamps.size(), convMetadata.size())
      << "Conversion timetamps and  arrays are not the same length.";

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
  auto attributionResultJson =
      folly::parseJson(fbpcf::io::read(inputSecretShareFilePath));

  for (const auto& [rule, formatters] : attributionResultJson.items()) {
    attributionRules_.push_back(rule.asString());
  }

  if (myRole == PUBLISHER) {
    touchpointSecretShare_ =
        AggregationMetrics::getAttributionsArrayfromDynamic(
            attributionResultJson);
  } else {
    conversionSecretShare_ =
        AggregationMetrics::getAttributionsArrayfromDynamic(
            attributionResultJson);
  }
}

} // namespace aggregation::private_aggregation
