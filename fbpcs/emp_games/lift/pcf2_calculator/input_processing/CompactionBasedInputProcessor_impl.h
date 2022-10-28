/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <algorithm>
#include <stdexcept>
#include "fbpcf/mpc_std_lib/util/secureRandomPermutation.h"
#include "fbpcs/emp_games/lift/pcf2_calculator/input_processing/CompactionBasedInputProcessor.h"
#include "fbpcs/emp_games/lift/pcf2_calculator/input_processing/IInputProcessor.h"

namespace private_lift {

template <int schedulerId>
std::vector<int32_t>
CompactionBasedInputProcessor<schedulerId>::shuffleAndGetUnionMap() {
  int32_t unionSize = inputData_.getNumRows();
  const std::vector<uint32_t> randomPermutation =
      fbpcf::mpc_std_lib::util::secureRandomPermutation(unionSize, *prg_);
  std::vector<int32_t> unionMap(unionSize);
  const std::vector<bool>& dummyRows = inputData_.getDummyRows();
  int32_t nonDummyRows = 0;
  for (int32_t i = 0; i < unionMap.size(); i++) {
    unionMap[randomPermutation[i]] =
        dummyRows[randomPermutation[i]] ? -1 : nonDummyRows++;
  }
  return unionMap;
}

template <int schedulerId>
std::vector<int32_t>
CompactionBasedInputProcessor<schedulerId>::getIntersectionMap(
    const std::vector<int32_t>& unionMap) {
  return adapter_->adapt(unionMap);
}

template <int schedulerId>
std::vector<std::vector<unsigned char>>
CompactionBasedInputProcessor<schedulerId>::preparePlaintextData(
    const std::vector<int32_t>& unionMap) {
  throw std::runtime_error("Not implemented");
}

template <int schedulerId>
std::pair<
    typename CompactionBasedInputProcessor<schedulerId>::SecString,
    typename CompactionBasedInputProcessor<schedulerId>::SecString>
CompactionBasedInputProcessor<schedulerId>::compactData(
    const std::vector<int32_t>& intersectionMap,
    const std::vector<std::vector<unsigned char>>& plaintextData) {
  int32_t myRows = plaintextData.size();

  auto publisherRows = common::shareIntFrom<
      schedulerId,
      sizeof(myRows) * 8,
      common::PUBLISHER,
      common::PARTNER>(myRole_, myRows);

  auto partnerRows = common::shareIntFrom<
      schedulerId,
      sizeof(myRows) * 8,
      common::PARTNER,
      common::PUBLISHER>(myRole_, myRows);
  SecString publisherDataShares;
  SecString partnerDataShares;

  if (myRole_ == common::PUBLISHER) {
    publisherDataShares =
        dataProcessor_->processMyData(plaintextData, intersectionMap.size());
    partnerDataShares = dataProcessor_->processPeersData(
        partnerRows,
        intersectionMap,
        PARTNER_CONVERSION_ROW_SIZE_BYTES * numConversionsPerUser_ +
            PARTNER_ROW_SIZE_BYTES);
  } else if (myRole_ == common::PARTNER) {
    publisherDataShares = dataProcessor_->processPeersData(
        publisherRows, intersectionMap, PUBLISHER_ROW_BYTES);
    partnerDataShares =
        dataProcessor_->processMyData(plaintextData, intersectionMap.size());
  }

  return std::make_pair<
      typename CompactionBasedInputProcessor<schedulerId>::SecString,
      typename CompactionBasedInputProcessor<schedulerId>::SecString>(
      std::move(publisherDataShares), std::move(partnerDataShares));
}

template <int schedulerId>
void CompactionBasedInputProcessor<schedulerId>::extractCompactedData(
    const typename CompactionBasedInputProcessor<schedulerId>::SecString&
        publisherDataShares,
    const typename CompactionBasedInputProcessor<schedulerId>::SecString&
        partnerDataShares) {
  throw std::runtime_error("Not implemented");
}

} // namespace private_lift
