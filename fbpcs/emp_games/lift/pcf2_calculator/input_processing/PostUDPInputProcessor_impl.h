/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <algorithm>
#include <functional>
#include <iterator>
#include <numeric>
#include <stdexcept>
#include <tuple>
#include <vector>

#include "fbpcf/mpc_std_lib/util/secureRandomPermutation.h"
#include "fbpcs/emp_games/lift/pcf2_calculator/input_processing/IInputProcessor.h"
#include "fbpcs/emp_games/lift/pcf2_calculator/input_processing/PostUDPInputProcessor.h"

namespace private_lift {

template <int schedulerId>
using SecString = typename fbpcf::mpc_std_lib::unified_data_process::
    data_processor::IDataProcessor<schedulerId>::SecString;

template <int schedulerId>
void PostUDPInputProcessor<schedulerId>::extractCompactedData(
    const SecString& publisherDataShares,
    const SecString& partnerDataShares) {
  input_processing::extractCompactedData(
      liftGameProcessedData_,
      controlPopulation_,
      cohortGroupIds_,
      breakdownBitGroupIds_,
      publisherDataShares,
      partnerDataShares,
      numConversionsPerUser_);
}

template <int schedulerId>
std::pair<
    typename PostUDPInputProcessor<schedulerId>::SecString,
    typename PostUDPInputProcessor<schedulerId>::SecString>
PostUDPInputProcessor<schedulerId>::fromMemoryToMPCTypes(
    const std::vector<std::vector<bool>>& publisherInputShares,
    const std::vector<std::vector<bool>>& partnerInputShares) {
  /* TODO (T145714644) complete the implementation of implement this function*/
  throw std::runtime_error("NOT_IMPLEMENTED");
}

} // namespace private_lift
