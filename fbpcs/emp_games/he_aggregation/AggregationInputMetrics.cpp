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

#include <fbpcf/io/api/FileIOWrappers.h>
#include "folly/json.h"
#include "folly/logging/xlog.h"

#include "fbpcs/emp_games/common/Constants.h"
#include "fbpcs/emp_games/common/Util.h"
#include "fbpcs/emp_games/he_aggregation/AggregationInputMetrics.h"
#include "fbpcs/emp_games/he_aggregation/AttributionAdditiveSSResult.h"
#include "fbpcs/emp_games/he_aggregation/HEAggOptions.h"
#include "fbpcs/emp_games/pcf2_aggregation/TouchpointMetadata.h"

// using namespace pcf2_aggregation;

namespace pcf2_he {

static const std::vector<pcf2_aggregation::TouchpointMetadata>
parseTouchpointMetadata(
    common::InputEncryption inputEncryption,
    const int lineNo,
    const std::vector<std::string>& header,
    const std::vector<std::string>& parts) {
  std::vector<uint64_t> adIds;
  std::vector<uint64_t> timestamps;
  std::vector<bool> isClicks;
  std::vector<uint64_t> campaignMetadata;

  CHECK_EQ(header.size(), parts.size())
      << "Error when reading csv file, header does not match parts.";

  for (size_t i = 0; i < header.size(); ++i) {
    std::string column = header[i];
    std::string value = parts[i];
    if (column == "ad_ids") {
      adIds = common::getInnerArray<uint64_t>(value);
    } else if (column == "timestamps") {
      timestamps = common::getInnerArray<uint64_t>(value);
    } else if (column == "is_click") {
      if (inputEncryption == common::InputEncryption::Xor) {
        // input is 64-bit secret shares
        std::vector<uint64_t> isClickShares =
            common::getInnerArray<uint64_t>(value);
        for (uint64_t isClickShare : isClickShares) {
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

  std::vector<pcf2_aggregation::TouchpointMetadata> tpms;
  for (size_t i = 0; i < adIds.size(); ++i) {
    tpms.push_back(pcf2_aggregation::TouchpointMetadata{
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
    tpms.push_back(pcf2_aggregation::TouchpointMetadata{0, 0, false, 0, 0});
  }

  return tpms;
}

// Secret share attribution result received by the game will be structured as
// : {"rule1" -> {"format1" -> {"pid1" -> {results}}}} Thus here, we are
// iterating over list of attribution results per pid per format per rule and
// adding them to a vector of maps from pid to vector<result>. While
// running the aggregation game, we will share this vector of vectors between
// parties (order maintained), where each vector would represent results for
// one rule and one format.
static std::vector<std::vector<std::vector<AttributionAdditiveSSResult>>>
getAttributionsArrayfromDynamic(const folly::dynamic& obj) {
  std::vector<std::map<int64_t, std::vector<AttributionAdditiveSSResult>>>
      attributionPidVectorMap;
  std::vector<std::string> attributionList; // list of attribution rules
  // For now, I am not using the rule name or formatter name in the logic as
  // the aggregation behaviour is not affected by different attribution rules.
  std::vector<std::vector<std::vector<AttributionAdditiveSSResult>>>
      attributionResultsList;
  for (const auto& [rule, formatters] : obj.items()) {
    attributionList.push_back(rule.asString());
    for (const auto& [formatter, resultPerPID] : formatters.items()) {
      std::map<int64_t, std::vector<AttributionAdditiveSSResult>>
          attributionsPerPidMap;
      for (const auto& [pid, results] : resultPerPID.items()) {
        std::vector<AttributionAdditiveSSResult> attributionResults;
        for (const auto& result : results) {
          attributionResults.push_back(
              AttributionAdditiveSSResult::fromDynamic(result));
        }
        attributionsPerPidMap.emplace(
            pid.asInt(), std::move(attributionResults));
      }
      attributionPidVectorMap.push_back(std::move(attributionsPerPidMap));
    }

    for (const auto& attributionsPerPidMap : attributionPidVectorMap) {
      std::vector<std::vector<AttributionAdditiveSSResult>>
          attributionPidVector;
      for (const auto& attributionResults : attributionsPerPidMap) {
        attributionPidVector.push_back(attributionResults.second);
      }
      attributionResultsList.push_back(std::move(attributionPidVector));
    }
  }

  return attributionResultsList;
}

AggregationInputMetrics::AggregationInputMetrics(
    common::InputEncryption inputEncryption,
    std::filesystem::path inputSecretShareFilePath,
    std::filesystem::path inputClearTextFilePath) {
  XLOGF(
      INFO,
      "Reading attribution result file {}",
      inputSecretShareFilePath.string());
  XLOGF(
      INFO, "Reading metadata input file {}", inputClearTextFilePath.string());
  XLOGF(
      INFO, "Parsing input metadata file {}", inputClearTextFilePath.string());

  // Parse the input metadata file
  int lineNo = 0;
  bool success = private_measurement::csv::readCsv(
      inputClearTextFilePath,
      [&](const std::vector<std::string>& header,
          const std::vector<std::string>& parts) {
        ids_.push_back(lineNo);

        touchpointMetadataArrays_.push_back(
            parseTouchpointMetadata(inputEncryption, lineNo, header, parts));

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

  attributionSecretShare_ =
      getAttributionsArrayfromDynamic(attributionResultJson);
}

} // namespace pcf2_he
