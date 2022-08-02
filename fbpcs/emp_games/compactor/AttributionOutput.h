/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once
#include "fbpcf/frontend/Bit.h"
#include "fbpcf/frontend/Int.h"
#include "fbpcs/emp_games/common/Csv.h"

namespace compactor {
template <int schedulerId>
using SecBitBatch = fbpcf::frontend::Bit<true, schedulerId, true>;

template <int schedulerId>
using SecUInt64Batch = fbpcf::frontend::Int<false, 64, true, schedulerId, true>;

template <int schedulerId>
using SecUInt32Batch = fbpcf::frontend::Int<false, 32, true, schedulerId, true>;

/* store plaintext of attribution output data*/
struct AttributionOutput {
  uint64_t adId;
  uint32_t conversionValue;
  bool isAttributed;

  AttributionOutput() {}

  AttributionOutput(uint64_t ad, uint32_t conv, bool label)
      : adId{ad}, conversionValue{conv}, isAttributed(label) {}
};

/* store 64 bits secret share of attribution output data */
struct AttributionOutputShare {
  uint64_t adId;
  uint64_t conversionValue;
  uint64_t isAttributed;

  AttributionOutputShare() {}

  AttributionOutputShare(uint64_t ad, uint64_t conv, uint64_t label)
      : adId{ad}, conversionValue{conv}, isAttributed(label) {}
};

/* store secret batch type of attribution output data */
template <int schedulerId>
struct SecretAttributionOutput {
  SecUInt64Batch<schedulerId> adId;
  SecUInt32Batch<schedulerId> conversionValue;
  SecBitBatch<schedulerId> isAttributed;

  SecretAttributionOutput() {}
  // process from plaintext
  SecretAttributionOutput(
      const std::vector<AttributionOutput>& src,
      int partyId) {
    std::vector<uint64_t> ad(src.size());
    std::vector<uint32_t> conv(src.size());
    std::vector<bool> label(src.size());

    for (size_t i = 0; i < src.size(); i++) {
      ad[i] = src.at(i).adId;
      conv[i] = src.at(i).conversionValue;
      label[i] = src.at(i).isAttributed;
    }

    adId = SecUInt64Batch<schedulerId>(ad, partyId);
    conversionValue = SecUInt32Batch<schedulerId>(conv, partyId);
    isAttributed = SecBitBatch<schedulerId>(label, partyId);
  }
  // process from secret shares
  explicit SecretAttributionOutput(
      const std::vector<AttributionOutputShare>& src) {
    std::vector<uint64_t> ad(src.size());
    std::vector<uint64_t> conv(src.size());
    std::vector<bool> label(src.size());

    for (size_t i = 0; i < src.size(); i++) {
      ad[i] = src.at(i).adId;
      conv[i] = src.at(i).conversionValue;
      label[i] = (src.at(i).isAttributed & 1); // lsb is sufficient
    }
    typename SecUInt64Batch<schedulerId>::ExtractedInt extractedAd(ad);
    adId = SecUInt64Batch<schedulerId>(std::move(extractedAd));
    typename SecUInt32Batch<schedulerId>::ExtractedInt extractedConv(conv);
    conversionValue = SecUInt32Batch<schedulerId>(std::move(extractedConv));
    typename SecBitBatch<schedulerId>::ExtractedBit extractedLabel(label);
    isAttributed = SecBitBatch<schedulerId>(std::move(extractedLabel));
  }
};

// Read XOR share from CSV file, where each row has a format of <adId,
// conversionValue, isAttributed>.
inline std::vector<AttributionOutputShare> readXORShareInput(
    std::filesystem::path filepath) {
  std::vector<AttributionOutputShare> res;

  bool success = private_measurement::csv::readCsv(
      filepath,
      [&](const std::vector<std::string>& /* header */,
          const std::vector<std::string>& row) {
        res.push_back(AttributionOutputShare(
            std::stoul(row[0]), std::stoul(row[1]), std::stoul(row[2])));
      });
  if (!success) {
    throw std::runtime_error("Failed to read input file " + filepath.string());
  }
  return res;
}

} // namespace compactor
