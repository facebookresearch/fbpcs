/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <gtest/gtest.h>
#include <future>

#include "fbpcs/emp_games/lift/pcf2_calculator/input_processing/LiftGameProcessedData.h"

// This file checks for input processor correctness based on
// ../../sample_input/publisher_unittest3.csv and
// ../../sample_input/partner_2_convs_unittest.csv
namespace private_lift::util {

template <int schedulerId>
inline void assertNumRows(
    const LiftGameProcessedData<schedulerId>& liftGameProcessedData) {
  EXPECT_EQ(liftGameProcessedData.numRows, 33);
}

template <int schedulerId>
inline void assertValueBits(
    const LiftGameProcessedData<schedulerId>& liftGameProcessedData) {
  EXPECT_EQ(liftGameProcessedData.valueBits, 10);
  EXPECT_EQ(liftGameProcessedData.valueSquaredBits, 15);
}

template <int schedulerId>
inline void assertPartnerCohorts(
    const LiftGameProcessedData<schedulerId>& liftGameProcessedData) {
  EXPECT_EQ(liftGameProcessedData.numPartnerCohorts, 3);
}

template <int schedulerId>
inline void assertNumBreakdowns(
    const LiftGameProcessedData<schedulerId>& liftGameProcessedData,
    bool computePublisherBreakdowns) {
  if (computePublisherBreakdowns) {
    EXPECT_EQ(liftGameProcessedData.numPublisherBreakdowns, 2);
  } else {
    EXPECT_EQ(liftGameProcessedData.numPublisherBreakdowns, 0);
  }
}

template <int schedulerId>
inline void assertNumGroups(
    const LiftGameProcessedData<schedulerId>& liftGameProcessedData,
    bool computePublisherBreakdowns) {
  if (computePublisherBreakdowns) {
    EXPECT_EQ(liftGameProcessedData.numGroups, 12);
  } else {
    EXPECT_EQ(liftGameProcessedData.numGroups, 6);
  }
}

template <int schedulerId>
inline void assertNumTestGroups(
    const LiftGameProcessedData<schedulerId>& liftGameProcessedData,
    bool computePublisherBreakdowns) {
  if (computePublisherBreakdowns) {
    EXPECT_EQ(liftGameProcessedData.numTestGroups, 7);
  } else {
    EXPECT_EQ(liftGameProcessedData.numTestGroups, 4);
  }
}

// Convert input boolean index shares to group ids
inline std::vector<uint32_t> convertIndexSharesToGroupIds(
    std::vector<std::vector<bool>> indexShares) {
  std::vector<uint32_t> groupIds;
  if (indexShares.size() == 0) {
    return groupIds;
  }
  for (auto i = 0; i < indexShares.at(0).size(); ++i) {
    uint32_t groupId = 0;
    for (auto j = 0; j < indexShares.size(); ++j) {
      groupId += indexShares.at(j).at(i) << j;
    }
    groupIds.push_back(groupId);
  }
  return groupIds;
}

template <int schedulerId>
inline void assertIndexShares(
    const LiftGameProcessedData<schedulerId>& publisherProcessedData,
    bool computePublisherBreakdowns,
    bool sortData = false) {
  auto indexShares = publisherProcessedData.indexShares;
  size_t groupWidth = std::ceil(std::log2(publisherProcessedData.numGroups));
  EXPECT_EQ(indexShares.size(), groupWidth);
  std::vector<uint32_t> expectGroupIds;
  if (computePublisherBreakdowns) {
    expectGroupIds = {3, 1, 9, 0, 0, 7, 1, 4, 6, 1, 4, 6, 3, 1, 7, 3, 3,
                      6, 0, 0, 6, 3, 3, 6, 3, 0, 2, 5, 3, 3, 5, 2, 11};
  } else {
    expectGroupIds = {0, 1, 3, 0, 0, 4, 1, 1, 3, 1, 1, 3, 0, 1, 4, 0, 0,
                      3, 0, 0, 3, 0, 0, 3, 0, 0, 2, 2, 0, 0, 2, 2, 5};
  }
  auto groupIds = util::convertIndexSharesToGroupIds(indexShares);

  if (sortData) {
    std::sort(expectGroupIds.begin(), expectGroupIds.end());
    std::sort(groupIds.begin(), groupIds.end());
  }

  EXPECT_EQ(expectGroupIds, groupIds);
}

template <int schedulerId>
inline void assertTestIndexShares(
    const LiftGameProcessedData<schedulerId>& publisherProcessedData,
    bool computePublisherBreakdowns,
    bool sortData = false) {
  auto testIndexShares = publisherProcessedData.testIndexShares;
  size_t testGroupWidth =
      std::ceil(std::log2(publisherProcessedData.numTestGroups));
  EXPECT_EQ(testIndexShares.size(), testGroupWidth);
  std::vector<uint32_t> expectTestGroupIds;
  if (computePublisherBreakdowns) {
    expectTestGroupIds = {3, 1, 6, 0, 0, 6, 1, 4, 6, 1, 4, 6, 3, 1, 6, 3, 3,
                          6, 0, 0, 6, 3, 3, 6, 3, 0, 2, 5, 3, 3, 5, 2, 6};
  } else {
    expectTestGroupIds = {0, 1, 3, 0, 0, 3, 1, 1, 3, 1, 1, 3, 0, 1, 3, 0, 0,
                          3, 0, 0, 3, 0, 0, 3, 0, 0, 2, 2, 0, 0, 2, 2, 3};
  }
  auto testGroupIds = util::convertIndexSharesToGroupIds(testIndexShares);

  if (sortData) {
    std::sort(expectTestGroupIds.begin(), expectTestGroupIds.end());
    std::sort(testGroupIds.begin(), testGroupIds.end());
  }

  EXPECT_EQ(expectTestGroupIds, testGroupIds);
}

inline void assertOpportunityTimestamps(
    const LiftGameProcessedData<0>& publisherData,
    const LiftGameProcessedData<1>& partnerData,
    bool sortData = false) {
  auto future0 = std::async([&] {
    return publisherData.opportunityTimestamps.openToParty(0).getValue();
  });
  auto future1 = std::async([&] {
    return partnerData.opportunityTimestamps.openToParty(0).getValue();
  });

  auto opportunityTimestamps = future0.get();
  future1.get();

  std::vector<uint64_t> expectOpportunityTimestamps = {
      0,   0,   0,   100, 100, 100, 100, 100, 100, 100, 100,
      100, 100, 100, 100, 100, 100, 100, 100, 100, 100, 100,
      100, 100, 0,   100, 100, 100, 100, 100, 100, 100, 100};

  if (sortData) {
    std::sort(
        expectOpportunityTimestamps.begin(), expectOpportunityTimestamps.end());
    std::sort(opportunityTimestamps.begin(), opportunityTimestamps.end());
  }

  EXPECT_EQ(opportunityTimestamps, expectOpportunityTimestamps);
}

inline void assertIsValidOpportunityTimestamps(
    const LiftGameProcessedData<0>& publisherData,
    const LiftGameProcessedData<1>& partnerData,
    bool sortData = false) {
  auto future0 = std::async([&] {
    return publisherData.isValidOpportunityTimestamp.openToParty(0).getValue();
  });
  auto future1 = std::async([&] {
    return partnerData.isValidOpportunityTimestamp.openToParty(0).getValue();
  });

  auto isValidOpportunityTimestamp = future0.get();
  future1.get();

  std::vector<bool> expectIsValidOpportunityTimestamp = {
      0, 0, 0, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1,
      1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 1, 1, 1, 0, 1, 1};

  if (sortData) {
    std::sort(
        expectIsValidOpportunityTimestamp.begin(),
        expectIsValidOpportunityTimestamp.end());
    std::sort(
        isValidOpportunityTimestamp.begin(), isValidOpportunityTimestamp.end());
  }
  EXPECT_EQ(isValidOpportunityTimestamp, expectIsValidOpportunityTimestamp);
}

template <int schedulerId>
inline std::vector<std::vector<uint64_t>> revealTimestamps(
    std::reference_wrapper<const std::vector<SecTimestamp<schedulerId>>>
        timestamps) {
  std::vector<std::vector<uint64_t>> result;
  for (size_t i = 0; i < timestamps.get().size(); ++i) {
    result.push_back(
        std::move(timestamps.get().at(i).openToParty(0).getValue()));
  }
  return result;
}

inline void assertPurchaseTimestamps(
    const LiftGameProcessedData<0>& publisherData,
    const LiftGameProcessedData<1>& partnerData,
    bool sortData = false) {
  auto future0 = std::async(
      revealTimestamps<0>,
      std::reference_wrapper<const std::vector<SecTimestamp<0>>>(
          publisherData.purchaseTimestamps));
  auto future1 = std::async(
      revealTimestamps<1>,
      std::reference_wrapper<const std::vector<SecTimestamp<1>>>(
          partnerData.purchaseTimestamps));
  auto purchaseTimestamps = future0.get();
  future1.get();
  std::vector<std::vector<uint64_t>> expectPurchaseTimestamps = {
      {0,  0,  0,  0,  0, 0, 0, 0, 0, 0, 0,   0,  150, 150, 150, 50, 50,
       50, 30, 30, 30, 0, 0, 0, 0, 0, 0, 150, 50, 30,  0,   0,   0},
      {100, 100, 100, 50,  50,  50,  100, 100, 100, 90,  90,
       90,  200, 200, 200, 150, 150, 150, 50,  50,  50,  0,
       0,   0,   100, 50,  150, 200, 150, 50,  200, 200, 200}};

  if (sortData) {
    for (int i = 0; i < expectPurchaseTimestamps.size(); i++) {
      std::sort(
          expectPurchaseTimestamps[i].begin(),
          expectPurchaseTimestamps[i].end());
      std::sort(purchaseTimestamps[i].begin(), purchaseTimestamps[i].end());
    }
  }

  EXPECT_EQ(purchaseTimestamps, expectPurchaseTimestamps);
}

inline void assertThresholdTimestamps(
    const LiftGameProcessedData<0>& publisherData,
    const LiftGameProcessedData<1>& partnerData,
    bool sortData = false) {
  auto future0 = std::async(
      revealTimestamps<0>,
      std::reference_wrapper<const std::vector<SecTimestamp<0>>>(
          publisherData.thresholdTimestamps));
  auto future1 = std::async(
      revealTimestamps<1>,
      std::reference_wrapper<const std::vector<SecTimestamp<1>>>(
          partnerData.thresholdTimestamps));
  auto thresholdTimestamps = future0.get();
  future1.get();
  std::vector<std::vector<uint64_t>> expectThresholdTimestamps = {
      {0,  0,  0,  0,  0, 0, 0, 0, 0, 0, 0,   0,  160, 160, 160, 60, 60,
       60, 40, 40, 40, 0, 0, 0, 0, 0, 0, 160, 60, 40,  0,   0,   0},
      {110, 110, 110, 60,  60,  60,  110, 110, 110, 100, 100,
       100, 210, 210, 210, 160, 160, 160, 60,  60,  60,  0,
       0,   0,   110, 60,  160, 210, 160, 60,  210, 210, 210}};

  if (sortData) {
    for (int i = 0; i < expectThresholdTimestamps.size(); i++) {
      std::sort(
          expectThresholdTimestamps[i].begin(),
          expectThresholdTimestamps[i].end());
      std::sort(thresholdTimestamps[i].begin(), thresholdTimestamps[i].end());
    }
  }
  EXPECT_EQ(thresholdTimestamps, expectThresholdTimestamps);
}

inline void assertAnyValidPurchaseTimestamp(
    const LiftGameProcessedData<0>& publisherData,
    const LiftGameProcessedData<1>& partnerData,
    bool sortData = false) {
  auto future0 = std::async([&] {
    return publisherData.anyValidPurchaseTimestamp.openToParty(0).getValue();
  });
  auto future1 = std::async([&] {
    return partnerData.anyValidPurchaseTimestamp.openToParty(0).getValue();
  });
  auto anyValidPurchaseTimestamp = future0.get();
  future1.get();
  std::vector<bool> expectAnyValidPurchaseTimestamp = {
      1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
      1, 1, 1, 1, 0, 0, 0, 1, 1, 1, 1, 1, 1, 1, 1, 1};

  if (sortData) {
    std::sort(
        expectAnyValidPurchaseTimestamp.begin(),
        expectAnyValidPurchaseTimestamp.end());
    std::sort(
        anyValidPurchaseTimestamp.begin(), anyValidPurchaseTimestamp.end());
  }
  EXPECT_EQ(anyValidPurchaseTimestamp, expectAnyValidPurchaseTimestamp);
}

template <int schedulerId>
inline std::vector<std::vector<int64_t>> revealValues(
    std::reference_wrapper<const std::vector<SecValue<schedulerId>>> values) {
  std::vector<std::vector<int64_t>> result;
  for (size_t i = 0; i < values.get().size(); ++i) {
    result.push_back(std::move(values.get().at(i).openToParty(0).getValue()));
  }
  return result;
}

inline void assertPurchaseValues(
    const LiftGameProcessedData<0>& publisherData,
    const LiftGameProcessedData<1>& partnerData,
    bool sortData = false) {
  auto future0 = std::async(
      revealValues<0>,
      std::reference_wrapper<const std::vector<SecValue<0>>>(
          publisherData.purchaseValues));
  auto future1 = std::async(
      revealValues<1>,
      std::reference_wrapper<const std::vector<SecValue<1>>>(
          partnerData.purchaseValues));
  auto purchaseValues = future0.get();
  future1.get();
  std::vector<std::vector<int64_t>> expectPurchaseValues = {
      {0,  0,  0,  0,  0, 0, 0, 0, 0, 0, 0,  0,  10, 10, 10, 10, 10,
       10, 10, 10, 10, 0, 0, 0, 0, 0, 0, 10, 10, 10, 0,  0,  0},
      {0,  0,  0,  20, 20, 20, 20, 20, 20, 20, 20, 20, 20, 20,  20,  20, 20,
       20, 20, 20, 20, 0,  0,  0,  50, 50, 50, 20, 20, 20, -50, -50, -50}};

  if (sortData) {
    for (int i = 0; i < expectPurchaseValues.size(); i++) {
      std::sort(expectPurchaseValues[i].begin(), expectPurchaseValues[i].end());
      std::sort(purchaseValues[i].begin(), purchaseValues[i].end());
    }
  }
  EXPECT_EQ(purchaseValues, expectPurchaseValues);
}

inline void assertReach(
    const LiftGameProcessedData<0>& publisherData,
    const LiftGameProcessedData<1>& partnerData,
    bool sortData = false) {
  auto future0 = std::async(
      [&] { return publisherData.testReach.openToParty(0).getValue(); });
  auto future1 = std::async(
      [&] { return partnerData.testReach.openToParty(0).getValue(); });
  auto testReach = future0.get();
  future1.get();

  std::vector<bool> expectTestReach = {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                                       0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                                       0, 0, 1, 1, 1, 1, 1, 1, 0, 0, 0};

  if (sortData) {
    std::sort(expectTestReach.begin(), expectTestReach.end());
    std::sort(testReach.begin(), testReach.end());
  }

  EXPECT_EQ(testReach, expectTestReach);
}

template <int schedulerId>
inline std::vector<std::vector<int64_t>> revealValueSquared(
    std::reference_wrapper<const std::vector<SecValueSquared<schedulerId>>>
        values) {
  std::vector<std::vector<int64_t>> result;
  for (size_t i = 0; i < values.get().size(); ++i) {
    result.push_back(std::move(values.get().at(i).openToParty(0).getValue()));
  }
  return result;
}

inline void assertPurchaseValuesSquared(
    const LiftGameProcessedData<0>& publisherData,
    const LiftGameProcessedData<1>& partnerData,
    bool sortData = false) {
  auto future0 = std::async(
      revealValueSquared<0>,
      std::reference_wrapper<const std::vector<SecValueSquared<0>>>(
          publisherData.purchaseValueSquared));
  auto future1 = std::async(
      revealValueSquared<1>,
      std::reference_wrapper<const std::vector<SecValueSquared<1>>>(
          partnerData.purchaseValueSquared));
  auto purchaseValueSquared = future0.get();
  future1.get();
  // squared sum of purchase value in each row
  std::vector<std::vector<int64_t>> expectPurchaseValueSquared = {
      {0,   0,   0,    400,  400,  400, 400, 400, 400,  400,  400,
       400, 900, 900,  900,  900,  900, 900, 900, 900,  900,  0,
       0,   0,   2500, 2500, 2500, 900, 900, 900, 2500, 2500, 2500},
      {0,   0,   0,    400,  400,  400, 400, 400, 400,  400,  400,
       400, 400, 400,  400,  400,  400, 400, 400, 400,  400,  0,
       0,   0,   2500, 2500, 2500, 400, 400, 400, 2500, 2500, 2500}};

  if (sortData) {
    for (int i = 0; i < expectPurchaseValueSquared.size(); i++) {
      std::sort(
          expectPurchaseValueSquared[i].begin(),
          expectPurchaseValueSquared[i].end());
      std::sort(purchaseValueSquared[i].begin(), purchaseValueSquared[i].end());
    }
  }
  EXPECT_EQ(purchaseValueSquared, expectPurchaseValueSquared);
}

} // namespace private_lift::util
