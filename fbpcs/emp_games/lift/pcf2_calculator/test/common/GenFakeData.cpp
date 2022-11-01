/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <algorithm>
#include <filesystem>
#include <stdexcept>
#include <string>

#include "folly/Random.h"

#include "fbpcf/io/api/BufferedWriter.h"
#include "fbpcf/io/api/FileIOWrappers.h"
#include "fbpcf/io/api/FileWriter.h"
#include "fbpcs/emp_games/lift/pcf2_calculator/test/common/GenFakeData.h"

namespace private_lift {
double GenFakeData::genAdjustedPurchaseRate(
    bool isTest,
    double purchaseRate,
    double incrementalityRate) {
  double adjstedPurchaseRate = purchaseRate;
  if (isTest) {
    adjstedPurchaseRate += (incrementalityRate / 2.0);
    if (adjstedPurchaseRate > 1.0) {
      throw std::invalid_argument(
          ">1.0 incrementality_rate + purchase_rate is not yet supported");
    }
  } else {
    adjstedPurchaseRate -= (incrementalityRate / 2.0);
    if (adjstedPurchaseRate < 0.0) {
      throw std::invalid_argument(
          "Incrementality rate cannot be significantly higher than the purchase rate");
    }
  }
  return adjstedPurchaseRate;
}

GenFakeData::LiftInputColumns GenFakeData::genOneFakeLine(
    const std::string& id,
    double opportunityRate,
    double testRate,
    double purchaseRate,
    double incrementalityRate,
    int32_t epoch,
    int32_t numConversions) {
  LiftInputColumns oneLine;
  oneLine.id = id;
  oneLine.opportunity = folly::Random::secureRandDouble01() < opportunityRate;
  purchaseRate = genAdjustedPurchaseRate(
      oneLine.test_flag, purchaseRate, incrementalityRate);
  bool hasPurchase = folly::Random::secureRandDouble01() < purchaseRate;

  // Lift input has an invariant that each PID must have an opportunity or
  // timestamp
  if (!oneLine.opportunity && !hasPurchase) {
    if (folly::Random::secureOneIn(2)) {
      oneLine.opportunity = true;
    } else {
      hasPurchase = true;
    }
  }
  oneLine.test_flag =
      oneLine.opportunity && folly::Random::secureRandDouble01() < testRate;
  oneLine.opportunity_timestamp =
      oneLine.opportunity ? folly::Random::secureRand32(1, 100) + epoch : 0;
  if (oneLine.test_flag) {
    oneLine.num_impressions = folly::Random::secureRand64(0, 5);
    oneLine.num_clicks = folly::Random::secureRand64(0, 5);
    oneLine.total_spend = folly::Random::secureRand64(0, 1000);
  } else {
    // the control group doesn't have engagement data since they don't see ads
    oneLine.num_impressions = 0;
    oneLine.num_clicks = 0;
    oneLine.total_spend = 0;
  }

  if (!hasPurchase) {
    oneLine.event_timestamps.resize(numConversions, 0);
    oneLine.values.resize(numConversions, 0);
  } else {
    uint32_t randomCount =
        numConversions - folly::Random::secureRand32(numConversions);
    std::vector<std::pair<int32_t, int32_t>> tsValVec;
    for (int32_t i = 0; i < numConversions; i++) {
      if (randomCount > 0) {
        int32_t timeStamp = folly::Random::secureRand32(1, 100) + epoch;
        int32_t value = folly::Random::secureRand32(100) + 1;
        tsValVec.push_back(std::make_pair(timeStamp, value));
        randomCount--;
      } else {
        tsValVec.push_back(std::make_pair(0, 0));
      }
    }

    // sort by timestamp
    std::sort(tsValVec.begin(), tsValVec.end(), [](auto& a, auto& b) {
      return a.first < b.first;
    });

    for (auto [ts, value] : tsValVec) {
      oneLine.event_timestamps.push_back(ts);
      oneLine.values.push_back(value);
    }
  }

  return oneLine;
}

void GenFakeData::genFakeInputFiles(
    const std::string& publisherInputFile,
    const std::string& partnerInputFile,
    const LiftFakeDataParams& params) {
  auto partnerFileWriter =
      std::make_unique<fbpcf::io::FileWriter>(partnerInputFile);
  auto partnerBufferedWriter =
      std::make_unique<fbpcf::io::BufferedWriter>(std::move(partnerFileWriter));

  // partner header: id_,event_timestamps,values,cohort_id
  std::string partnerFileHeader = "id_,event_timestamps";
  if (!params.omitValuesColumn_) {
    partnerFileHeader += ",values";
  }

  if (params.numCohorts_) {
    partnerFileHeader += ",cohort_id";
  }
  partnerFileHeader += '\n';
  partnerBufferedWriter->writeString(partnerFileHeader);

  auto publisherFileWriter =
      std::make_unique<fbpcf::io::FileWriter>(publisherInputFile);
  auto publisherBufferedWriter = std::make_unique<fbpcf::io::BufferedWriter>(
      std::move(publisherFileWriter));

  // publisher header: id_,opportunity,test_flag,opportunity_timestamp,
  //   num_impressions,num_clicks
  std::string publisherFileHeader;
  if (!params.numBreakdowns_) {
    publisherFileHeader =
        "id_,opportunity,test_flag,opportunity_timestamp,num_impressions,num_clicks,total_spend\n";
  } else {
    // only include breakdown_id if numBreakdowns_ is present
    publisherFileHeader =
        "id_,opportunity,test_flag,opportunity_timestamp,num_impressions,num_clicks,total_spend,breakdown_id\n";
  }
  publisherBufferedWriter->writeString(publisherFileHeader);

  for (std::size_t i = 0; i < params.numRows_; i++) {
    // generate one row of fake data
    LiftInputColumns oneLine = genOneFakeLine(
        std::to_string(i),
        params.opportunityRate_,
        params.testRate_,
        params.purchaseRate_,
        params.incrementalityRate_,
        params.epoch_,
        params.numConversions_);

    // write one row to partner fake data file
    std::string eventTSString = "[";
    std::string valuesString = "[";
    for (auto j = 0; j < params.numConversions_; j++) {
      eventTSString += std::to_string(oneLine.event_timestamps[j]);
      valuesString += std::to_string(oneLine.values[j]);
      if (j < params.numConversions_ - 1) {
        eventTSString += ",";
        valuesString += ",";
      } else {
        eventTSString += "]";
        valuesString += "]";
      }
    }
    std::string partnerLine = oneLine.id + "," + eventTSString;
    if (!params.omitValuesColumn_) {
      partnerLine += "," + valuesString;
    }
    if (params.numCohorts_) {
      // generate a random cohort id between 0 and numCohorts - 1
      int32_t randomCohortId =
          folly::Random::secureRand32(0, params.numCohorts_);
      partnerLine += "," + std::to_string(randomCohortId);
    }
    partnerLine += '\n';
    partnerBufferedWriter->writeString(partnerLine);

    // write one row to publisher fake data file
    std::string publisherRow = oneLine.id + "," +
        (oneLine.opportunity ? "1," : "0,") +
        (oneLine.test_flag ? "1," : "0,") +
        std::to_string(oneLine.opportunity_timestamp) + "," +
        std::to_string(oneLine.num_impressions) + "," +
        std::to_string(oneLine.num_clicks) + "," +
        std::to_string(oneLine.total_spend);

    if (params.numBreakdowns_) {
      // generate a random breakdown id between 0 and numBreakdown - 1
      int32_t randomBreakdownId =
          folly::Random::secureRand32(0, params.numBreakdowns_);
      publisherRow += "," + std::to_string(randomBreakdownId);
    }
    publisherRow += '\n';
    publisherBufferedWriter->writeString(publisherRow);
  }
  partnerBufferedWriter->close();
  publisherBufferedWriter->close();
}
} // namespace private_lift
