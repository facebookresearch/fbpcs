/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <cstdint>
#include <string>
#include <vector>

struct FakeDataGeneratorParams {
  std::vector<std::string> header;
  double opportunityRate = 0.8;
  double testRate = 0.5;
  double purchaseRate = 0.1;
  double incrementalityRate = 0.1;
  // 2020-09-13 12:26:40 UTC
  // Just a nice round number near the current date
  int64_t minTs = 1600000000;
  // 30 days after the default minTs
  int64_t maxTs = 1600000000 + 86400 * 30;
  bool shouldUseMd5Ids = true;
  int16_t numConversions = 4;

  FakeDataGenerator(std::vector<std::string> header_) : header{header_} {}

  FakeDataGeneratorParams& withOpportunityRate(double r) {
    opportunityRate = r;
    return *this;
  }

  FakeDataGeneratorParams& withTestRate(double r) {
    testRate = r;
    return *this;
  }

  FakeDataGeneratorParams& withPurchaseRate(double r) {
    purchaseRate = r;
    return *this;
  }

  FakeDataGeneratorParams& withIncrementalityRate(double r) {
    incrementalityRate = r;
    return *this;
  }

  FakeDataGeneratorParams& withMinTs(int64_t ts) {
    minTs = ts;
    return *this;
  }

  FakeDataGeneratorParams& withMaxTs(int64_t ts) {
    maxTs = ts;
    return *this;
  }

  FakeDataGeneratorParams& withShouldUseMd5Ids(bool b) {
    shouldUseMd5Ids = b;
    return *this;
  }

  FakeDataGeneratorParams& withNumConversions(int16_t n) {
    numConversions = n;
    return *this;
  }
}
