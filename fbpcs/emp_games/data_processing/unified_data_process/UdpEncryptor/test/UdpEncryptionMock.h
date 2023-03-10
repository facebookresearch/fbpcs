/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include "fbpcf/mpc_std_lib/unified_data_process/data_processor/IUdpEncryption.h"

namespace unified_data_process {

using namespace ::testing;

class UdpEncryptionMock final
    : public fbpcf::mpc_std_lib::unified_data_process::data_processor::
          IUdpEncryption {
 public:
  MOCK_METHOD(void, prepareToProcessMyData, (size_t));

  MOCK_METHOD(
      void,
      processMyData,
      (const std::vector<std::vector<unsigned char>>&));

  MOCK_METHOD(std::vector<__m128i>, getExpandedKey, ());

  MOCK_METHOD(
      void,
      prepareToProcessPeerData,
      (size_t, const std::vector<int32_t>&));

  MOCK_METHOD(void, processPeerData, (size_t));

  MOCK_METHOD(EncryptionResuts, getProcessedData, ());
};

} // namespace unified_data_process
