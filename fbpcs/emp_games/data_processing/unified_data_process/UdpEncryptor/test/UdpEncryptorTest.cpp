/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "fbpcs/emp_games/data_processing/unified_data_process/UdpEncryptor/UdpEncryptor.h"
#include "fbpcs/emp_games/data_processing/unified_data_process/UdpEncryptor/test/UdpEncryptionMock.h"

#include <gtest/gtest.h>
#include <memory>

using namespace ::testing;
namespace unified_data_process {

TEST(UdpEncryptorTest, testProcessingPeerData) {
  int chunkSize = 500;
  int totalRow = 1200;
  size_t dataWidth = 32;
  std::vector<int32_t> indexes{3, 31, 6, 12, 5};
  auto mock = std::make_unique<unified_data_process::UdpEncryptionMock>();

  EXPECT_CALL(*mock, prepareToProcessPeerData(dataWidth, indexes)).Times(1);
  EXPECT_CALL(*mock, processPeerData(chunkSize)).Times(totalRow / chunkSize);
  if (totalRow % chunkSize != 0) {
    EXPECT_CALL(*mock, processPeerData(totalRow % chunkSize)).Times(1);
  }
  EXPECT_CALL(*mock, getProcessedData()).Times(1);

  UdpEncryptor encryptor(std::move(mock), chunkSize);
  encryptor.setPeerConfig(totalRow, dataWidth, indexes);
  encryptor.getEncryptionResults();
}

} // namespace unified_data_process
