/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <chrono>
#include <cstdint>
#include <random>
#include <string>
#include <vector>

enum class Role { Publisher, Partner };

struct FakeDataGeneratorParams {
  Role role;
  std::vector<std::string> header;
  double opportunityRate = 0.8;
  double testRate = 0.9;
  double purchaseRate = 0.1;
  // 2020-09-13 12:26:40 UTC
  // Just a nice round number near the current date
  int64_t minTs = 1'600'000'000;
  // 30 days after the default minTs
  int64_t maxTs = 1'600'000'000 + 86400 * 30;
  int64_t minValue = 100;
  int64_t maxValue = 10000;
  bool shouldUseComplexIds = true;

  FakeDataGeneratorParams(Role role_, std::vector<std::string> header_)
      : role{role_}, header{header_} {}

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

  FakeDataGeneratorParams& withMinTs(int64_t ts) {
    minTs = ts;
    return *this;
  }

  FakeDataGeneratorParams& withMaxTs(int64_t ts) {
    maxTs = ts;
    return *this;
  }

  FakeDataGeneratorParams& withMinValue(int64_t v) {
    minValue = v;
    return *this;
  }

  FakeDataGeneratorParams& withMaxValue(int64_t v) {
    maxValue = v;
    return *this;
  }

  FakeDataGeneratorParams& withShouldUseComplexIds(bool b) {
    shouldUseComplexIds = b;
    return *this;
  }
};

class FakeDataGenerator {
 public:
  explicit FakeDataGenerator(FakeDataGeneratorParams params)
      : FakeDataGenerator{
            params,
            static_cast<uint32_t>(
                std::chrono::system_clock::now().time_since_epoch().count())} {}

  FakeDataGenerator(FakeDataGeneratorParams params, uint32_t seed)
      : params_{params}, r_{seed}, n_{0} {}

  std::string genOneRow();

 private:
  FakeDataGeneratorParams params_;
  std::default_random_engine r_;
  int64_t n_;
};
