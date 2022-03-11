/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <re2/re2.h>
#include <filesystem>
#include <fstream>
#include <map>
#include <unordered_set>

#include "fbpcs/emp_games/common/Constants.h"
#include "fbpcs/emp_games/common/Util.h"
#include "fbpcs/emp_games/pcf2_attribution/AttributionOptions.h"

namespace pcf2_attribution {

template <bool usingBatch, common::InputEncryption inputEncryption>
const std::vector<ParsedTouchpoint>
AttributionInputMetrics<usingBatch, inputEncryption>::parseTouchpoints(
    const int myRole,
    const int lineNo,
    const std::vector<std::string>& header,
    const std::vector<std::string>& parts) {
  std::vector<uint64_t> timestamps;
  std::vector<bool> isClicks;

  for (auto i = 0; i < header.size(); ++i) {
    auto column = header[i];
    auto value = parts[i];
    if (column == "timestamps") {
      timestamps = common::getInnerArray<uint64_t>(value);
    } else if (column == "is_click") {
      if constexpr (inputEncryption == common::InputEncryption::Xor) {
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
    }
  }

  CHECK_EQ(timestamps.size(), isClicks.size())
      << "timestamps arrays and is_click arrays are not the same length.";
  CHECK_LE(timestamps.size(), FLAGS_max_num_touchpoints)
      << "Number of touchpoints exceeds the maximum allowed value.";

  std::vector<int64_t> unique_ids;
  for (auto i = 0; i < timestamps.size(); ++i) {
    unique_ids.push_back(i);
  }

  const std::unordered_set<int64_t> idSet{unique_ids.begin(), unique_ids.end()};
  CHECK_EQ(idSet.size(), timestamps.size())
      << "Found non-unique id for line " << lineNo << ". "
      << "This implementation currently only supports unique touchpoint ids per user.";

  std::vector<ParsedTouchpoint> tps;
  for (auto i = 0; i < timestamps.size(); ++i) {
    tps.push_back(ParsedTouchpoint{
        /* id */ unique_ids.at(i),
        /* isClick */ isClicks.at(i) == 1,
        /* ts */ timestamps.at(i)});
  }

  // The input received by attribution game from data processing is sorted by
  // rows, but in each row the internal columns are not sorted. Thus sorting the
  // touchpoints based on timestamp, where views come before clicks.
  // If the input is encrypted, the sorting has to be done in the data
  // processing step.
  if constexpr (inputEncryption != common::InputEncryption::Xor) {
    std::sort(tps.begin(), tps.end());
  }

  // Add padding at the end of the input data for publisher; partner data
  // consists only of padded data
  for (auto i = tps.size(); i < FLAGS_max_num_touchpoints; ++i) {
    tps.push_back(ParsedTouchpoint{-1, false, 0});
  }
  return tps;
}

template <bool usingBatch, common::InputEncryption inputEncryption>
const std::vector<ParsedConversion>
AttributionInputMetrics<usingBatch, inputEncryption>::parseConversions(
    const int myRole,
    const std::vector<std::string>& header,
    const std::vector<std::string>& parts) {
  std::vector<uint64_t> convTimestamps;

  for (auto i = 0; i < header.size(); ++i) {
    auto column = header[i];
    auto value = parts[i];

    if (column == "conversion_timestamps") {
      convTimestamps = common::getInnerArray<uint64_t>(value);
    }
  }

  CHECK_LE(convTimestamps.size(), FLAGS_max_num_conversions)
      << "Number of conversions exceeds the maximum allowed value.";

  std::vector<ParsedConversion> convs;
  for (auto i = 0; i < convTimestamps.size(); ++i) {
    convs.push_back(ParsedConversion{convTimestamps.at(i)});
  }

  // Sorting conversions based on timestamp. If the input is encrypted, this has
  // to be done in the data processing step.
  if constexpr (inputEncryption == common::InputEncryption::Plaintext) {
    std::sort(convs.begin(), convs.end());
  }

  // Add padding at the end of the input data for partner; publisher data
  // consists only of padded data
  for (auto i = convs.size(); i < FLAGS_max_num_conversions; ++i) {
    // Add padding
    convs.push_back(ParsedConversion{0});
  }
  return convs;
}

template <bool usingBatch, common::InputEncryption inputEncryption>
const std::vector<TouchpointT<usingBatch>>
AttributionInputMetrics<usingBatch, inputEncryption>::
    convertParsedTouchpointsToTouchpoints(
        const std::vector<std::vector<ParsedTouchpoint>>& parsedTouchpoints) {
  std::vector<TouchpointT<usingBatch>> touchpoints;

  if constexpr (usingBatch) {
    std::vector<std::vector<int64_t>> ids(
        FLAGS_max_num_touchpoints, std::vector<int64_t>{});
    std::vector<std::vector<bool>> isClicks(
        FLAGS_max_num_touchpoints, std::vector<bool>{});
    std::vector<std::vector<uint64_t>> timestamps(
        FLAGS_max_num_touchpoints, std::vector<uint64_t>{});

    // The touchpoints are parsed row by row, whereas the batches are across
    // rows.
    for (size_t i = 0; i < parsedTouchpoints.size(); ++i) {
      for (size_t j = 0; j < FLAGS_max_num_touchpoints; ++j) {
        auto parsedTouchpoint = parsedTouchpoints.at(i).at(j);
        ids.at(j).push_back(parsedTouchpoint.id);
        isClicks.at(j).push_back(parsedTouchpoint.isClick);
        timestamps.at(j).push_back(parsedTouchpoint.ts);
      }
    }
    for (size_t i = 0; i < FLAGS_max_num_touchpoints; ++i) {
      touchpoints.push_back(
          Touchpoint<true>{ids.at(i), isClicks.at(i), timestamps.at(i)});
    }
  } else {
    for (size_t i = 0; i < parsedTouchpoints.size(); ++i) {
      std::vector<Touchpoint<false>> touchpointRow;
      for (auto& parsedTouchpoint : parsedTouchpoints.at(i)) {
        touchpointRow.push_back(Touchpoint<false>{
            parsedTouchpoint.id,
            parsedTouchpoint.isClick,
            parsedTouchpoint.ts});
      }
      touchpoints.push_back(std::move(touchpointRow));
    }
  }
  return touchpoints;
}

template <bool usingBatch, common::InputEncryption inputEncryption>
const std::vector<ConversionT<usingBatch>>
AttributionInputMetrics<usingBatch, inputEncryption>::
    convertParsedConversionsToConversions(
        const std::vector<std::vector<ParsedConversion>>& parsedConversions) {
  std::vector<ConversionT<usingBatch>> conversions;

  if constexpr (usingBatch) {
    std::vector<std::vector<uint64_t>> timestamps(
        FLAGS_max_num_conversions, std::vector<uint64_t>{});

    // The conversions are parsed row by row, whereas the batches are across
    // rows.
    for (size_t i = 0; i < parsedConversions.size(); ++i) {
      for (size_t j = 0; j < parsedConversions.at(i).size(); ++j) {
        timestamps.at(j).push_back(parsedConversions.at(i).at(j).ts);
      }
    }
    for (size_t i = 0; i < timestamps.size(); ++i) {
      conversions.push_back(Conversion<true>{timestamps.at(i)});
    }
  } else {
    for (size_t i = 0; i < parsedConversions.size(); ++i) {
      std::vector<Conversion<false>> conversionRow;
      for (auto& parsedConversion : parsedConversions.at(i)) {
        conversionRow.push_back(Conversion<false>{parsedConversion.ts});
      }
      conversions.push_back(std::move(conversionRow));
    }
  }
  return conversions;
}

template <bool usingBatch, common::InputEncryption inputEncryption>
AttributionInputMetrics<usingBatch, inputEncryption>::AttributionInputMetrics(
    int myRole,
    std::string attributionRulesStr,
    std::filesystem::path filepath) {
  XLOGF(INFO, "Reading CSV {}", filepath.string());

  // Parse the passed attribution rules
  if (myRole == common::PUBLISHER) {
    attributionRules_ =
        private_measurement::csv::splitByComma(attributionRulesStr, false);
  }

  // Parse the input CSV
  std::vector<std::vector<ParsedTouchpoint>> parsedTouchpoints;
  std::vector<std::vector<ParsedConversion>> parsedConversions;
  auto lineNo = 0;
  bool success = private_measurement::csv::readCsv(
      filepath,
      [&](const std::vector<std::string>& header,
          const std::vector<std::string>& parts) {
        if (lineNo == 0) {
          XLOGF(DBG, "{}", common::vecToString(header));
        }
        XLOGF(DBG, "{}: {}", lineNo, common::vecToString(parts));
        ids_.push_back(lineNo);

        parsedTouchpoints.push_back(
            parseTouchpoints(myRole, lineNo, header, parts));
        parsedConversions.push_back(parseConversions(myRole, header, parts));

        lineNo++;
      });

  if (!success) {
    XLOGF(FATAL, "Failed to read input file {},", filepath.string());
  }

  // Convert from parsed touchpoints and conversions to touchpoints and
  // conversions
  tpArrays_ = convertParsedTouchpointsToTouchpoints(parsedTouchpoints);
  convArrays_ = convertParsedConversionsToConversions(parsedConversions);
}

} // namespace pcf2_attribution
