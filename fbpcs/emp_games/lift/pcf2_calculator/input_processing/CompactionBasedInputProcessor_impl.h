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
#include <unordered_map>
#include <vector>

#include "fbpcf/mpc_std_lib/util/secureRandomPermutation.h"
#include "fbpcs/emp_games/lift/pcf2_calculator/input_processing/CompactionBasedInputProcessor.h"
#include "fbpcs/emp_games/lift/pcf2_calculator/input_processing/IInputProcessor.h"
#include "fbpcs/emp_games/lift/pcf2_calculator/input_processing/serialization/LiftMetaDataSerializer.h"

namespace private_lift {

template <int schedulerId>
std::vector<int32_t>
CompactionBasedInputProcessor<schedulerId>::shuffleAndGetUnionMap() {
  XLOG(INFO) << "Shuffling input and preparing Union Map for Adapter input";
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
  XLOG(INFO) << "Begin adapter protocol";
  return adapter_->adapt(unionMap);
}

template <int schedulerId>
std::vector<std::vector<unsigned char>>
CompactionBasedInputProcessor<schedulerId>::preparePlaintextData(
    const std::vector<int32_t>& unionMap) {
  XLOG(INFO) << "Begin plaintext data serialization as bytes";
  size_t unionSize = inputData_.getNumRows();
  int32_t inputSize = 0;
  std::vector<int32_t> reverseUnionMap(unionSize);

  for (int i = 0; i < unionMap.size(); i++) {
    if (unionMap[i] >= 0) {
      reverseUnionMap[unionMap[i]] = i;
      inputSize = std::max(inputSize, unionMap[i]);
    }
  }

  inputSize++;
  reverseUnionMap.resize(inputSize);

  if (myRole_ == common::PARTNER) {
    // Construct a serializer
    LiftMetaDataSerializer partnerSerializer(
        inputData_, numConversionsPerUser_, reverseUnionMap, unionSize);
    return partnerSerializer.serializePartnerMetadata();
  } else {
    LiftMetaDataSerializer publisherSerializer(
        inputData_, numConversionsPerUser_, reverseUnionMap, unionSize);
    return publisherSerializer.serializePublisherMetadata();
  }
}

template <int schedulerId>
std::pair<
    typename CompactionBasedInputProcessor<schedulerId>::SecString,
    typename CompactionBasedInputProcessor<schedulerId>::SecString>
CompactionBasedInputProcessor<schedulerId>::compactData(
    const std::vector<int32_t>& intersectionMap,
    const std::vector<std::vector<unsigned char>>& plaintextData) {
  XLOG(INFO) << "Beginning oblivious data intersection step";

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

  auto publisherSerializer =
      input_processing::createPublisherSerializer<schedulerId>(
          numConversionsPerUser_);

  auto partnerSerializer =
      input_processing::createPartnerSerializer<schedulerId>(
          numConversionsPerUser_);

  XLOG(INFO) << "Publisher Row count: " << publisherRows;
  XLOG(INFO) << "Publisher Row size in bytes: "
             << publisherSerializer->getRowSizeBytes();

  XLOG(INFO) << "Partner Row count: " << partnerRows;
  XLOG(INFO) << "Partner Row size in bytes: "
             << partnerSerializer->getRowSizeBytes();

  SecString publisherDataShares;
  SecString partnerDataShares;

  if (myRole_ == common::PUBLISHER) {
    XLOG(INFO) << "Begin processing my data (publisher)";
    publisherDataShares =
        dataProcessor_->processMyData(plaintextData, intersectionMap.size());
    XLOG(INFO) << "Begin processing peers data (partner)";
    partnerDataShares = dataProcessor_->processPeersData(
        partnerRows, intersectionMap, partnerSerializer->getRowSizeBytes());
  } else if (myRole_ == common::PARTNER) {
    XLOG(INFO) << "Begin processing peers data (publisher)";
    publisherDataShares = dataProcessor_->processPeersData(
        publisherRows, intersectionMap, publisherSerializer->getRowSizeBytes());
    XLOG(INFO) << "Begin processing my data (partner)";
    partnerDataShares =
        dataProcessor_->processMyData(plaintextData, intersectionMap.size());
  }

  auto expectedIntersectionSize = std::transform_reduce(
      intersectionMap.begin(),
      intersectionMap.end(),
      0,
      [](const int32_t& left, const int32_t& right) { return left + right; },
      [](const int32_t& ele) { return ele == -1 ? 0 : 1; });

  if (expectedIntersectionSize != publisherDataShares.getBatchSize()) {
    throw std::runtime_error(folly::sformat(
        "Publisher rows do not match up expected intersection size. Expected {} but got {} rows.",
        expectedIntersectionSize,
        publisherDataShares.getBatchSize()));
  }

  if (expectedIntersectionSize != partnerDataShares.getBatchSize()) {
    throw std::runtime_error(folly::sformat(
        "Partner rows do not match up expected intersection size. Expected {} but got {} rows.",
        expectedIntersectionSize,
        partnerDataShares.getBatchSize()));
  }

  XLOG(INFO) << folly::format(
      "{} rows in intersection after running data processor",
      expectedIntersectionSize);

  return std::make_pair<
      typename CompactionBasedInputProcessor<schedulerId>::SecString,
      typename CompactionBasedInputProcessor<schedulerId>::SecString>(
      std::move(publisherDataShares), std::move(partnerDataShares));
}
} // namespace private_lift
