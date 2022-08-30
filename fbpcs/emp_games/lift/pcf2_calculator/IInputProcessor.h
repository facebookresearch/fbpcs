/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <fbpcs/emp_games/lift/pcf2_calculator/Constants.h>
#include <cstdint>
#include <vector>
namespace private_lift {

template <int schedulerId>
class IInputProcessor {
 public:
  virtual ~IInputProcessor() = default;

  virtual int64_t getNumRows() const = 0;

  virtual uint32_t getNumPartnerCohorts() const = 0;

  virtual uint32_t getNumPublisherBreakdowns() const = 0;

  virtual uint32_t getNumGroups() const = 0;

  virtual uint32_t getNumTestGroups() const = 0;

  virtual uint8_t getValueBits() const = 0;

  virtual uint8_t getValueSquaredBits() const = 0;

  virtual const std::vector<std::vector<bool>>& getIndexShares() const = 0;

  virtual const std::vector<std::vector<bool>>& getTestIndexShares() const = 0;

  // TODO add better types to PCF and replace
  virtual const SecTimestamp<schedulerId>& getOpportunityTimestamps() const = 0;

  virtual const SecBit<schedulerId>& getIsValidOpportunityTimestamp() const = 0;

  virtual const std::vector<SecTimestamp<schedulerId>>& getPurchaseTimestamps()
      const = 0;

  virtual const std::vector<SecTimestamp<schedulerId>>& getThresholdTimestamps()
      const = 0;

  virtual const SecBit<schedulerId>& getAnyValidPurchaseTimestamp() const = 0;

  virtual const std::vector<SecValue<schedulerId>>& getPurchaseValues()
      const = 0;

  virtual const std::vector<SecValueSquared<schedulerId>>&
  getPurchaseValueSquared() const = 0;

  virtual const SecBit<schedulerId>& getTestReach() const = 0;
};

} // namespace private_lift
