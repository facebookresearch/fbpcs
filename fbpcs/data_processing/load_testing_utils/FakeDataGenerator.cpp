/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "fbpcs/data_processing/load_testing_utils/FakeDataGenerator.h"

#include <algorithm>
#include <cstdint>
#include <random>
#include <string>
#include <unordered_map>

static std::string genIdFor(int64_t n) {
  auto c = std::to_string(n);
  // I'm too lazy to do something better
  // and md5 is a PITA without pulling in openssl
  return "a1" + c + "b2c3" + c + "d4" + c + "e5f6";
}

std::string FakeDataGenerator::genOneRow() {
  std::uniform_real_distribution<double> realDist{0, 1};
  std::uniform_int_distribution<int8_t> binaryDist{0, 1};
  // Used for impressions and clicks
  std::uniform_int_distribution<int64_t> engagementDist{0, 10};
  std::uniform_int_distribution<int64_t> valueDist{
      params_.minValue, params_.maxValue};
  std::uniform_int_distribution<int64_t> tsDist{params_.minTs, params_.maxTs};

  // Shared stuff
  auto id = params_.shouldUseComplexIds ? genIdFor(n_) : std::to_string(n_);
  auto groupId = binaryDist(r_);

  // Publisher stuff
  auto hasOpp = realDist(r_) < params_.opportunityRate ? 1 : 0;
  auto oppTs = hasOpp * tsDist(r_);
  auto isTest = hasOpp && realDist(r_) < params_.testRate ? 1 : 0;
  // Engagement
  auto impressions = isTest * engagementDist(r_);
  auto clicks = std::min(impressions, engagementDist(r_));
  auto spend = isTest * valueDist(r_);

  // Partner stuff
  auto hasPurchase = realDist(r_) < params_.purchaseRate ? 1 : 0;
  auto eventTs = hasPurchase * tsDist(r_);
  auto value = hasPurchase * valueDist(r_);

  // If no opp as publisher, useless row
  if (!hasOpp && params_.role == Role::Publisher) {
    return "";
  }

  // If no purchase as partner, useless row
  if (!hasPurchase && params_.role == Role::Partner) {
    return "";
  }

  std::string res;
  std::unordered_map<std::string, std::string> m{
      {"id_", id},
      {"opportunity_timestamp", std::to_string(oppTs)},
      {"test_flag", std::to_string(isTest)},
      {"num_impressions", std::to_string(impressions)},
      {"num_clicks", std::to_string(clicks)},
      {"total_spend", std::to_string(spend)},
      {"breakdown_id", std::to_string(groupId)},
      {"event_timestamp", std::to_string(eventTs)},
      {"value", std::to_string(value)},
      {"cohort_id", std::to_string(groupId)},
  };

  for (const auto& col : params_.header) {
    res += m.at(col);
    res += ',';
  }

  // Get rid of the last ','
  res.pop_back();

  ++n_;
  return res;
}
