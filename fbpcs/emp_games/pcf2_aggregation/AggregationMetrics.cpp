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
#include <unordered_map>
#include <utility>
#include <vector>

#include <fbpcf/io/FileManagerUtil.h>
#include <fbpcf/io/api/FileIOWrappers.h>
#include "folly/json.h"
#include "folly/logging/xlog.h"

#include "fbpcs/emp_games/common/Constants.h"
#include "fbpcs/emp_games/common/Util.h"
#include "fbpcs/emp_games/pcf2_aggregation/AggregationMetrics.h"
#include "fbpcs/emp_games/pcf2_aggregation/AggregationOptions.h"
#include "fbpcs/emp_games/pcf2_aggregation/AttributionResult.h"
#include "fbpcs/emp_games/pcf2_aggregation/Constants.h"
#include "fbpcs/emp_games/pcf2_aggregation/ConversionMetadata.h"
#include "fbpcs/emp_games/pcf2_aggregation/TouchpointMetadata.h"

namespace pcf2_aggregation {

static const std::vector<TouchpointMetadata> parseTouchpointMetadata(
    const int myRole,
    common::InputEncryption inputEncryption,
    const int lineNo,
    const std::vector<std::string>& header,
    const std::vector<std::string>& parts) {
  std::vector<uint64_t> adIds;
  std::vector<uint64_t> timestamps;
  std::vector<bool> isClicks;
  std::vector<uint64_t> campaignMetadata;

  for (size_t i = 0; i < header.size(); ++i) {
    auto column = header[i];
    auto value = parts[i];
    if (column == "ad_ids") {
      adIds = common::getInnerArray<uint64_t>(value);
    } else if (column == "timestamps") {
      timestamps = common::getInnerArray<uint64_t>(value);
    } else if (column == "is_click") {
      if (inputEncryption == common::InputEncryption::Xor) {
        // input is 64-bit secret shares
        std::vector<uint64_t> isClickShares =
            common::getInnerArray<uint64_t>(value);
        for (auto isClickShare : isClickShares) {
          // suffices to read last bit
          isClicks.push_back(isClickShare & 1);
        }
      } else {
        isClicks = common::getInnerArray<bool>(value);
      }
    } else if (column == "campaign_metadata") {
      campaignMetadata = common::getInnerArray<uint64_t>(value);
    }
  }

  CHECK_EQ(adIds.size(), timestamps.size())
      << "Ad ids and timestamps arrays are not the same length.";
  CHECK_EQ(adIds.size(), isClicks.size())
      << "Ad ids and is_click arrays are not the same length.";
  CHECK_EQ(adIds.size(), campaignMetadata.size())
      << "Ad ids and campaign_metadata arrays are not the same length.";
  CHECK_LE(adIds.size(), FLAGS_max_num_touchpoints)
      << "Number of touchpoints exceeds the maximum allowed value.";

  std::vector<int64_t> unique_ids;
  for (size_t i = 0; i < timestamps.size(); ++i) {
    unique_ids.push_back(i);
  }

  const std::unordered_set<int64_t> idSet{unique_ids.begin(), unique_ids.end()};
  CHECK_EQ(idSet.size(), timestamps.size())
      << "Found non-unique id for line " << lineNo << ". "
      << "This implementation currently only supports unique touchpoint ids per user.";

  std::vector<TouchpointMetadata> tpms;
  for (size_t i = 0; i < adIds.size(); ++i) {
    tpms.push_back(TouchpointMetadata{
        /* original adId */ adIds.at(i),
        /* ts */ timestamps.at(i),
        /* isClick */ isClicks.at(i) == 1,
        /* campaignMetadata */ campaignMetadata.at(i),
        /* compressed adId */ 0});
  }

  // Sort touchpoints so that metadata are aligned with order in attribution
  // game. If input is encrypted, we assume that the input is already sorted.
  if (inputEncryption != common::InputEncryption::Xor) {
    std::sort(tpms.begin(), tpms.end());
  }

  // Add padding at the end of the input data for publisher; partner data
  // consists only of padded data
  for (size_t i = tpms.size(); i < FLAGS_max_num_touchpoints; ++i) {
    tpms.push_back(TouchpointMetadata{0, 0, false, 0, 0});
  }

  return tpms;
}

// Aggregation Formats are received by publisher and will be shared to partner
// privately. We need to parse input data before that, so in this case we are
// extracting fields for all aggregators - currently measurement and PCM. During
// the game then, once aggregator formats are shared between both publisher and
// partner. We will then extract the fields required for only those aggregators.
static const std::vector<ConversionMetadata> parseConversionMetadata(
    const int myRole,
    common::InputEncryption inputEncryption,
    const std::vector<std::string>& header,
    const std::vector<std::string>& parts) {
  std::vector<uint64_t> convTimestamps;
  std::vector<uint64_t> convValues;
  std::vector<uint64_t> convMetadata;

  for (size_t i = 0; i < header.size(); ++i) {
    auto column = header[i];
    auto value = parts[i];

    if (column == "conversion_timestamps") {
      convTimestamps = common::getInnerArray<uint64_t>(value);
    } else if (column == "conversion_values") {
      convValues = common::getInnerArray<uint64_t>(value);
    } else if (column == "conversion_metadata") {
      convMetadata = common::getInnerArray<uint64_t>(value);
    }
  }

  CHECK_EQ(convTimestamps.size(), convValues.size())
      << "Conversion timetamps and conversion value arrays are not the same length.";
  CHECK_EQ(convTimestamps.size(), convMetadata.size())
      << "Conversion timetamps and  arrays are not the same length.";
  CHECK_LE(convTimestamps.size(), FLAGS_max_num_conversions)
      << "Number of conversions exceeds the maximum allowed value.";

  std::vector<ConversionMetadata> convs;
  for (size_t i = 0; i < convTimestamps.size(); ++i) {
    convs.push_back(ConversionMetadata{
        /* ts */ convTimestamps.at(i),
        /* value */
        static_cast<uint32_t>(
            convValues.at(i)), // since the inputs are 64 bit secret shares of a
                               // 32 bit integer, only the first 32 bits matter.
        /* metadata */ convMetadata.at(i),
        /* inputEncryption */ inputEncryption});
  }

  // Sort conversions to align with order in attribution game. If input is
  // encrypted, we assume that the input is already sorted.
  if (inputEncryption == common::InputEncryption::Plaintext) {
    std::sort(convs.begin(), convs.end());
  }

  // Add padding at the end of the input data for partner; publisher data
  // consists only of padded data
  for (size_t i = convs.size(); i < FLAGS_max_num_conversions; ++i) {
    // Add padding
    convs.push_back(ConversionMetadata{0, 0, 0, inputEncryption});
  }

  return convs;
}

AggregationInputMetrics::AggregationInputMetrics(
    int myRole,
    common::InputEncryption inputEncryption,
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

  aggregationFormats_ =
      private_measurement::csv::splitByComma(aggregationFormatNamesStr, false);
  if (myRole == common::PUBLISHER) {
    CHECK_GT(aggregationFormats_.size(), 0) << "No aggregation formats found";
  }

  // Parse the input metadata file
  auto lineNo = 0;
  auto success = private_measurement::csv::readCsv(
      inputClearTextFilePath,
      [&](const std::vector<std::string>& header,
          const std::vector<std::string>& parts) {
        ids_.push_back(lineNo);

        touchpointMetadataArrays_.push_back(parseTouchpointMetadata(
            myRole, inputEncryption, lineNo, header, parts));
        conversionMetadataArrays_.push_back(
            parseConversionMetadata(myRole, inputEncryption, header, parts));

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

  attributionSecretShare_ = AggregationMetrics::getAttributionsArrayfromDynamic(
      attributionResultJson);
}

} // namespace pcf2_aggregation
