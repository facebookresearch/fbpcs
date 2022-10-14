/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <emmintrin.h>
#include "fbpcf/engine/communication/IPartyCommunicationAgent.h"
#include "fbpcf/engine/util/util.h"
#include "fbpcf/mpc_std_lib/aes_circuit/AesCircuit_impl.h"
#include "fbpcf/mpc_std_lib/aes_circuit/IAesCircuitCtr.h"
#include "fbpcs/data_processing/unified_data_process/data_processor/IDataProcessor.h"
namespace unified_data_process::data_processor {

/**
 * This is the implementation of UDP data processor.
 */
template <int schedulerId>
class DataProcessor final : public IDataProcessor<schedulerId> {
 public:
  using AesCtr = fbpcf::mpc_std_lib::aes_circuit::IAesCircuitCtr<
      typename IDataProcessor<schedulerId>::SecBit>;

  explicit DataProcessor(
      int32_t myId,
      int32_t partnerId,
      std::unique_ptr<fbpcf::engine::communication::IPartyCommunicationAgent>
          agent,
      std::unique_ptr<AesCtr> aesCircuitCtr)
      : myId_(myId),
        partnerId_(partnerId),
        agent_(std::move(agent)),
        aesCircuitCtr_(std::move(aesCircuitCtr)) {}

  /**
   * @inherit doc
   */
  typename IDataProcessor<schedulerId>::SecString processMyData(
      const std::vector<std::vector<unsigned char>>& plaintextData,
      size_t outputSize) override;

  /**
   * @inherit doc
   */
  typename IDataProcessor<schedulerId>::SecString processPeersData(
      size_t dataSize,
      const std::vector<int32_t>& indexes,
      size_t dataWidth) override;

 private:
  int32_t myId_;
  int32_t partnerId_;
  std::unique_ptr<fbpcf::engine::communication::IPartyCommunicationAgent>
      agent_;
  std::unique_ptr<AesCtr> aesCircuitCtr_;

 protected:
  // locally encrypt the plaintext, output expanded keys and ciphertext
  std::tuple<std::array<__m128i, 11>, std::vector<std::vector<uint8_t>>>
  localEncryption(const std::vector<std::vector<unsigned char>>& plaintextData);

  // privately share the input byte stream from party inputPartyID into vector
  // of batched Bit. Also padding the Bit vector to make its size be mulitple
  // of 128
  std::vector<typename IDataProcessor<schedulerId>::SecBit>
  privatelyShareByteStream(
      const std::vector<std::vector<unsigned char>>& localData,
      int inputPartyID);

  // privately share a 2d vector of __m128i from party inputPartyID into vector
  // of batched Bit.
  std::vector<typename IDataProcessor<schedulerId>::SecBit>
  privatelyShareM128iStream(
      const std::vector<std::vector<__m128i>>& localDataM128i,
      int inputPartyID);

  // privately share the expanded key from party inputPartyID into vector
  // of batched Bit. Each bit from the expanded key will be convert into a
  // batched Bit with a specified bathcSize
  std::vector<typename IDataProcessor<schedulerId>::SecBit>
  privatelyShareExpandedKey(
      const std::vector<__m128i>& localKeyM128i,
      size_t batchSize,
      int inputPartyID);
};

} // namespace unified_data_process::data_processor
