/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include "fbpcf/engine/util/aes.h"
#include "fbpcf/mpc_std_lib/aes_circuit/AesCircuitCtr.h"
#include "fbpcf/mpc_std_lib/aes_circuit/AesCircuitCtr_impl.h"
#include "fbpcs/data_processing/unified_data_process/data_processor/DataProcessor.h"

namespace unified_data_process::data_processor {

template <int schedulerId>
typename IDataProcessor<schedulerId>::SecString
DataProcessor<schedulerId>::processMyData(
    const std::vector<std::vector<unsigned char>>& plaintextData,
    size_t outputSize) {
  size_t dataSize = plaintextData.size();
  size_t dataWidth = plaintextData.at(0).size();

  // 1a. encrypt my data locally
  auto keyAndCiphertext = localEncryption(plaintextData);
  auto& expandedKeyM128i = std::get<0>(keyAndCiphertext);
  auto& ciphertextByte = std::get<1>(keyAndCiphertext);

  // 2a. send encryted data to peer
  for (auto& item : ciphertextByte) {
    agent_->send(item);
  }

  // 1b. (peer)receive encryted data from peer
  // 2b. (peer)pick desired ciphertext blocks
  // 3a. share key
  std::vector<__m128i> expandedKeyVectorM128i(
      expandedKeyM128i.begin(), expandedKeyM128i.end());
  auto keyString =
      privatelyShareExpandedKey(expandedKeyVectorM128i, outputSize, myId_);

  // 3b. (peer)share ciphertext and mask
  std::vector<std::vector<unsigned char>> ciphertextPlaceholder(
      outputSize, std::vector<unsigned char>(ciphertextByte.at(0).size()));
  auto filteredCiphertext =
      privatelyShareByteStream(ciphertextPlaceholder, partnerId_);

  std::vector<std::vector<__m128i>> countersPlaceholderM128i(
      outputSize, std::vector<__m128i>(filteredCiphertext.size() / 128));
  auto filteredCounters =
      privatelyShareM128iStream(countersPlaceholderM128i, partnerId_);

  // 4a/b. decryt the data jointly (input my key privately)
  auto decryptData =
      aesCircuitCtr_->decrypt(filteredCiphertext, keyString, filteredCounters);

  // reverse each byte from little endian into big endian order
  std::vector<typename IDataProcessor<schedulerId>::SecBit> reversedData(
      decryptData.size());
  for (int i = 0; i < decryptData.size(); ++i) {
    reversedData[8 * (i / 8) + (7 - i % 8)] = decryptData[i];
  }
  // 5a/b. output decrypted data
  // remove the trailing padding bits
  typename IDataProcessor<schedulerId>::SecString outputShare(dataWidth * 8);
  for (size_t i = 0; i < dataWidth * 8; ++i) {
    outputShare[i] = reversedData[i];
  }
  return outputShare;
}

template <int schedulerId>
typename IDataProcessor<schedulerId>::SecString
DataProcessor<schedulerId>::processPeersData(
    size_t dataSize,
    const std::vector<int64_t>& indexes,
    size_t dataWidth) {
  // 1a. (peer)encrypt my data locally
  // 2a. (peer)send encryted data to peer
  // 1b. receive encryted data from peer
  size_t intersectionSize = indexes.size();
  std::vector<std::vector<unsigned char>> ciphertextByte(
      dataSize, std::vector<unsigned char>(dataWidth));
  for (size_t i = 0; i < dataSize; i++) {
    ciphertextByte[i] = agent_->receive(dataWidth);
  }

  // 2b. pick desired ciphertext blocks
  std::vector<std::vector<unsigned char>> intersection(
      intersectionSize, std::vector<unsigned char>(dataWidth));
  for (size_t i = 0; i < intersectionSize; ++i) {
    intersection[i] = ciphertextByte[indexes[i]];
  }

  // 3a. (peer)share key
  std::vector<__m128i> keyPlaceholderM128i(11);
  auto keyString = privatelyShareExpandedKey(
      keyPlaceholderM128i, intersectionSize, partnerId_);

  // 3b. share ciphertext and mask
  size_t cipherWidth =
      dataWidth % 16 == 0 ? dataWidth : dataWidth + 16 - dataWidth % 16;
  size_t cipherBlocks = cipherWidth / 16;
  auto filteredCiphertext = privatelyShareByteStream(intersection, myId_);

  std::vector<std::vector<__m128i>> filteredCountersM128i(
      intersectionSize, std::vector<__m128i>(cipherBlocks));
  for (uint64_t i = 0; i < intersectionSize; ++i) {
    for (uint64_t j = 0; j < cipherBlocks; ++j) {
      filteredCountersM128i[i][j] =
          _mm_set_epi64x(0, indexes[i] * cipherBlocks + j);
    }
  }
  auto filteredCounters =
      privatelyShareM128iStream(filteredCountersM128i, myId_);

  // 4a/b. decryt the picked blocks jointly (input the ciphertext and mask
  // privately)
  auto decryptData =
      aesCircuitCtr_->decrypt(filteredCiphertext, keyString, filteredCounters);

  // reverse each byte from little endian into big endian order
  std::vector<typename IDataProcessor<schedulerId>::SecBit> reversedData(
      decryptData.size());
  for (size_t i = 0; i < decryptData.size(); ++i) {
    reversedData[8 * (i / 8) + (7 - i % 8)] = decryptData[i];
  }

  // 5a/b. output decrypted data
  // remove the trailing padding bits
  typename IDataProcessor<schedulerId>::SecString outputShare(dataWidth * 8);
  for (size_t i = 0; i < dataWidth * 8; ++i) {
    outputShare[i] = reversedData[i];
  }
  return outputShare;
}

template <int schedulerId>
std::tuple<std::array<__m128i, 11>, std::vector<std::vector<uint8_t>>>
DataProcessor<schedulerId>::localEncryption(
    const std::vector<std::vector<unsigned char>>& plaintextData) {
  size_t rowCounts = plaintextData.size();
  size_t rowSize = plaintextData.at(0).size();
  size_t rowBlocks = rowSize / 16 + (rowSize % 16 != 0);

  __m128i keyM128i = fbpcf::engine::util::getRandomM128iFromSystemNoise();
  fbpcf::engine::util::Aes localAes(keyM128i);
  auto expandedKeyM128i = localAes.expandEncryptionKey(keyM128i);
  // generate counters for each block
  std::vector<__m128i> counterM128i(rowCounts * rowBlocks);
  for (uint64_t i = 0; i < counterM128i.size(); ++i) {
    counterM128i[i] = _mm_set_epi64x(0, i);
  }
  // encrypt counters
  localAes.encryptInPlace(counterM128i);

  std::vector<uint8_t> maskByte;
  maskByte.reserve(counterM128i.size() * 16);
  for (auto unit : counterM128i) {
    uint8_t tmparray[16];
    _mm_storeu_si128((__m128i*)tmparray, unit);
    maskByte.insert(maskByte.end(), &tmparray[0], &tmparray[16]);
  }

  std::vector<std::vector<uint8_t>> ciphertextByte(
      rowCounts, std::vector<uint8_t>(rowSize));
  for (size_t i = 0; i < rowCounts; ++i) {
    for (size_t j = 0; j < rowSize; ++j) {
      ciphertextByte[i][j] =
          plaintextData[i][j] ^ maskByte[i * rowBlocks * 16 + j];
    }
  }
  return {expandedKeyM128i, ciphertextByte};
}

template <int schedulerId>
std::vector<typename IDataProcessor<schedulerId>::SecBit>
DataProcessor<schedulerId>::privatelyShareByteStream(
    const std::vector<std::vector<unsigned char>>& localData,
    int inputPartyID) {
  size_t unitSize = sizeof(unsigned char) * 8;
  size_t localDataWidth = localData.at(0).size() * unitSize;
  size_t stringWidth = localDataWidth % 128 == 0
      ? localDataWidth
      : localDataWidth + 128 - localDataWidth % 128;
  size_t inputSize = localData.size();
  std::vector<typename IDataProcessor<schedulerId>::SecBit> sharedData(
      stringWidth);
  for (size_t i = 0; i < localDataWidth; i++) {
    std::vector<bool> sharedBit(inputSize);
    for (size_t j = 0; j < inputSize; j++) {
      sharedBit[j] =
          (localData[j][i / unitSize] >> (unitSize - 1 - i % unitSize)) & 1;
    }
    sharedData[i] =
        typename IDataProcessor<schedulerId>::SecBit(sharedBit, inputPartyID);
  }
  // padding
  for (size_t i = localDataWidth; i < stringWidth; ++i) {
    std::vector<bool> sharedBit(inputSize);
    sharedData[i] =
        typename IDataProcessor<schedulerId>::SecBit(sharedBit, inputPartyID);
  }
  return sharedData;
}

template <int schedulerId>
std::vector<typename IDataProcessor<schedulerId>::SecBit>
DataProcessor<schedulerId>::privatelyShareM128iStream(
    const std::vector<std::vector<__m128i>>& localDataM128i,
    int inputPartyID) {
  size_t unitSize = 128;
  size_t batchSize = localDataM128i.size();
  size_t rowSize = localDataM128i.at(0).size();
  std::vector<std::vector<bool>> localDataBool(
      batchSize * rowSize, std::vector<bool>(unitSize));
  for (size_t i = 0; i < batchSize; ++i) {
    for (size_t j = 0; j < rowSize; ++j) {
      // The bits extracted from extractLnbToVector() is the following order:
      // All bytes are in a order that from most significant byte to least
      // significant bytes. The bits in each byte is in a order that from lsb to
      // msb.
      fbpcf::engine::util::extractLnbToVector(
          localDataM128i[i][j], localDataBool[i * rowSize + j]);
    }
  }
  std::vector<typename IDataProcessor<schedulerId>::SecBit> sharedDataBit(
      rowSize * 128);
  for (size_t i = 0; i < rowSize * 128; ++i) {
    std::vector<bool> sharedBit(batchSize);
    for (size_t j = 0; j < batchSize; ++j) {
      sharedBit[j] =
          localDataBool[j * rowSize + i / 128][i % 128 / 8 * 8 + (7 - i % 8)];
    }
    sharedDataBit[i] =
        typename IDataProcessor<schedulerId>::SecBit(sharedBit, inputPartyID);
  }
  return sharedDataBit;
}

template <int schedulerId>
std::vector<typename IDataProcessor<schedulerId>::SecBit>
DataProcessor<schedulerId>::privatelyShareExpandedKey(
    const std::vector<__m128i>& localKeyM128i,
    size_t batchSize,
    int inputPartyID) {
  size_t unitSize = 128;
  size_t blockNo = localKeyM128i.size(); // should be 11
  std::vector<std::vector<bool>> localDataBool(
      blockNo, std::vector<bool>(unitSize));
  for (size_t i = 0; i < blockNo; ++i) {
    // The bits extracted from extractLnbToVector() is the following order:
    // All bytes are in a order that from most significant byte to least
    // significant bytes. The bits in each byte is in a order that from lsb to
    // msb.
    fbpcf::engine::util::extractLnbToVector(localKeyM128i[i], localDataBool[i]);
  }
  std::vector<typename IDataProcessor<schedulerId>::SecBit> sharedKeyBit(
      blockNo * unitSize);
  for (size_t i = 0; i < blockNo * unitSize; ++i) {
    std::vector<bool> sharedBit(
        batchSize,
        localDataBool[i / unitSize][i % unitSize / 8 * 8 + (7 - i % 8)]);
    sharedKeyBit[i] =
        typename IDataProcessor<schedulerId>::SecBit(sharedBit, inputPartyID);
  }
  return sharedKeyBit;
}
} // namespace unified_data_process::data_processor
