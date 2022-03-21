/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <stdint.h>
#include <vector>
#include "fbpcf/frontend/BitString.h"

namespace unified_data_process::data_processor {

/**
 * A data processor can generate the secret shares of the data of the matched
 * rows based on the indexes of those rows provided by one party and the actual
 * data provided by the other.
 */
template <int schedulerId>
class IDataProcessor {
 public:
  using SecString = fbpcf::frontend::BitString<true, schedulerId, true>;
  using PubString = fbpcf::frontend::BitString<false, schedulerId, true>;

  virtual ~IDataProcessor() = default;

  /**
   * Process this party's data and generate the secret shares of the data of the
   * matched rows. The other party will provide the indexes of those rows.
   * @param plaintextData my data
   * @param outputSize how many rows are expected to appear in the output
   * @return the secret-shared values of the data of the matched rows
   */
  virtual SecString processMyData(
      const std::vector<std::vector<unsigned char>>& plaintextData,
      size_t outputSize) = 0;

  /**
   * Process the other party's data and generate the secret shares of the data
   * of the matched rows. The other party will provide the data while this party
   * will specify the indexes of the matched rows.
   * @param dataSize how many rows are expected from the other party
   * @param indexes the indexes of the matched rows, the order matters
   * @param dataWidth how many bytes are there in the data.
   * @return the secret-shared values of the data of the matched rows
   */
  virtual SecString processPeersData(
      size_t dataSize,
      const std::vector<int64_t>& indexes,
      size_t dataWidth) = 0;
};

} // namespace unified_data_process::data_processor
