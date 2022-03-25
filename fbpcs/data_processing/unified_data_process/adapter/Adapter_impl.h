/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <cmath>
#include <stdexcept>
#include "fbpcs/data_processing/unified_data_process/adapter/Adapter.h"

namespace unified_data_process::adapter {

template <int schedulerId>
std::vector<int64_t> Adapter<schedulerId>::adapt(
    const std::vector<int64_t>& unionMap) const {
  auto unionSize = unionMap.size();

  if (unionSize == 0) {
    throw std::runtime_error("Union size can not be 0.");
  }
  size_t indexWidth = std::ceil(std::log2(unionSize));

  SecString ids(2 * indexWidth + 1);

  std::vector<bool> hasValue(unionSize);
  std::vector<uint64_t> myMap(unionSize);

  for (size_t i = 0; i < unionSize; i++) {
    hasValue[i] = unionMap.at(i) >= 0;
    myMap[i] = unionMap.at(i) >= 0 ? unionMap.at(i) : 0;
  }

  // This bit indicates whether both parties have value. If only one party has
  // value, then this bit will be 0; if both parties have value, this bit will
  // be 1. It is impossible that neither party has value.
  ids[0] = !SecBit(typename SecBit::ExtractedBit(hasValue));
  for (size_t i = 0; i < indexWidth; i++) {
    std::vector<bool> myShare(unionSize);
    for (size_t j = 0; j < unionSize; j++) {
      myShare[j] = (myMap[j] >> i) & 1;
    }
    ids[i + 1] = SecBit(myShare, party0Id_);
    ids[1 + indexWidth + i] = SecBit(myShare, party1Id_);
  }

  auto shuffledIds = shuffler_->shuffle(std::move(ids), unionSize);
  auto match0 = shuffledIds[0].openToParty(party0Id_);
  auto match1 = shuffledIds[0].openToParty(party1Id_);
  auto matchResult =
      amIParty0_ ? std::move(match0).getValue() : std::move(match1).getValue();
  int64_t intersectionSize = 0;
  for (auto item : matchResult) {
    intersectionSize += item;
  }
  std::vector<std::vector<bool>> share0Plaintext(
      indexWidth, std::vector<bool>(intersectionSize));
  std::vector<std::vector<bool>> share1Plaintext(
      indexWidth, std::vector<bool>(intersectionSize));
  for (size_t i = 0; i < indexWidth; i++) {
    auto firstShare = shuffledIds[1 + i].extractBit().getValue();
    auto secondShare = shuffledIds[indexWidth + 1 + i].extractBit().getValue();
    int index = 0;
    for (size_t j = 0; j < unionSize; j++) {
      if (matchResult.at(j)) {
        share0Plaintext[i][index] = firstShare.at(j);
        share1Plaintext[i][index] = secondShare.at(j);
        index++;
      }
    }
  }

  SecString share0(
      typename SecString::ExtractedString(std::move(share0Plaintext)));
  SecString share1(
      typename SecString::ExtractedString(std::move(share1Plaintext)));
  auto myShare0 = share0.openToParty(party1Id_);
  auto myShare1 = share1.openToParty(party0Id_);
  auto myShare = amIParty0_ ? std::move(myShare1).getValue()
                            : std::move(myShare0).getValue();

  std::vector<int64_t> rst(intersectionSize, 0);
  for (size_t i = 0; i < intersectionSize; i++) {
    for (size_t j = 0; j < indexWidth; j++) {
      rst[i] += (myShare.at(i).at(j)) << j;
    }
  }
  return rst;
}

} // namespace unified_data_process::adapter
