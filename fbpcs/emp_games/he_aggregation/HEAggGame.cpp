/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "fbpcs/emp_games/he_aggregation/HEAggGame.h"
#include <cstdint>
#include "fbpcf/engine/util/AesPrgFactory.h"
#include "fbpcf/mpc_std_lib/oram/DifferenceCalculatorFactory.h"
#include "fbpcf/mpc_std_lib/oram/LinearOramFactory.h"
#include "fbpcf/mpc_std_lib/oram/ObliviousDeltaCalculatorFactory.h"
#include "fbpcf/mpc_std_lib/oram/SinglePointArrayGeneratorFactory.h"
#include "fbpcf/mpc_std_lib/oram/WriteOnlyOramFactory.h"
#include "fbpcs/emp_games/he_aggregation/AttributionAdditiveSSResult.h"
#include "fbpcs/emp_games/he_aggregation/HEAggOptions.h"

#include "folly/logging/xlog.h"

#include "privacy_infra/elgamal/ElGamal.h"

namespace pcf2_he {

std::vector<uint8_t> encryptAttrResult(
    heschme::PublicKey& pk,
    const AggregationInputMetrics& input,
    int maxTouchpoints,
    int maxConversions) {
  std::vector<std::vector<std::vector<AttributionAdditiveSSResult>>>
      secretShareAttributionArrays = input.getAttributionSecretShares();
  std::vector<uint8_t> ciphertextArray;
  for (const auto& secretShareAttributionArray : secretShareAttributionArrays) {
    for (const auto& paddedSecretAttribution : secretShareAttributionArray) {
      // Each touchpoint has is_attr ss for each conversion. We add all
      // is_attr ss for the same touchpoint in plaintext before HE encryption
      for (int i = 0; i < maxTouchpoints; i++) {
        int partnerAttrResult = 0;
        for (int j = i; j < maxConversions * maxTouchpoints;
             j += maxTouchpoints) {
          partnerAttrResult += paddedSecretAttribution[j].isAttributed;
        }
        std::vector<uint8_t> c = pk.encrypt(partnerAttrResult).toBytes();
        ciphertextArray.insert(ciphertextArray.end(), c.begin(), c.end());
      }
    }
  }
  return ciphertextArray;
}
std::vector<uint64_t> decryptAggCiphertext(
    heschme::PrivateKey& sk,
    const std::vector<uint8_t>& aggregatedCiphertexts,
    int numGroups,
    int ciphertextSize) {
  std::vector<uint64_t> decryptedArray;

  // Initialize pointers to the ciphertext
  int ciphertextStart = 0;
  int ciphertextEnd = ciphertextStart + ciphertextSize;

  for (int j = 0; j < numGroups; j++) {
    // initialize the ciphertext based on the pointers' location
    const std::vector<uint8_t> c(
        aggregatedCiphertexts.begin() + ciphertextStart,
        aggregatedCiphertexts.begin() + ciphertextEnd);
    heschme::Ciphertext aggregatedCiphertext =
        heschme::Ciphertext::fromBytes(c);

    // Decrypt ciphertext
    uint64_t decrypted = sk.decrypt(aggregatedCiphertext);
    decryptedArray.push_back(std::move(decrypted));

    // advance the ciphertext pointers
    ciphertextStart += ciphertextSize;
    ciphertextEnd += ciphertextSize;
  }
  return decryptedArray;
}

std::unordered_map<uint64_t, heschme::Ciphertext> aggregateCiphertexts(
    const std::vector<uint8_t>& ciphertextArray,
    const AggregationInputMetrics& input,
    int maxTouchpoints,
    int maxConversions,
    int ciphertextSize) {
  std::unordered_map<uint64_t, heschme::Ciphertext> adIdToAggregate;

  auto& touchpointMetadataArrays = input.getTouchpointMetadata();
  auto& secretShareAttributionArrays = input.getAttributionSecretShares();

  int ciphertextStart = 0;
  int ciphertextEnd = ciphertextStart + ciphertextSize;
  for (int i = 0; i < touchpointMetadataArrays.size(); i++) {
    // get publisher side secret share for the first attr r
    auto& paddedSecretAttribution = secretShareAttributionArrays[0][i];
    auto& touchpointMetadataArray = touchpointMetadataArrays[i];

    for (int j = 0; j < touchpointMetadataArray.size(); j++) {
      // Each touchpoint has is_attr for each conversion. Add all is_attr for
      // the same touchpoint in plaintext
      uint64_t pubAttrResult = 0;
      for (int k = 0; k < maxConversions * maxTouchpoints;
           k += maxTouchpoints) {
        pubAttrResult += paddedSecretAttribution[k].isAttributed;
      }

      // initialize the ciphertext from received bytes
      std::vector<uint8_t> c(
          ciphertextArray.begin() + ciphertextStart,
          ciphertextArray.begin() + ciphertextEnd);
      heschme::Ciphertext partnerAttrValue = heschme::Ciphertext::fromBytes(c);

      // combine publisher and partner conv values in HE
      heschme::Ciphertext attrVal = heschme::Ciphertext::add_with_plaintext(
          partnerAttrValue, pubAttrResult);

      // find the adId to aggregate against
      int adId = touchpointMetadataArray[j].originalAdId;

      // add ciphertext to the adId bucket
      if (adIdToAggregate.find(adId) != adIdToAggregate.end()) {
        heschme::Ciphertext currentSum = adIdToAggregate[adId];
        adIdToAggregate[adId] =
            heschme::Ciphertext::add_with_ciphertext(currentSum, attrVal);
      } else {
        adIdToAggregate[adId] = attrVal;
      }

      // advance the ciphertext pointers
      ciphertextStart += ciphertextSize;
      ciphertextEnd += ciphertextSize;
    }
  }
  return adIdToAggregate;
}

std::unordered_map<uint64_t, uint64_t> HEAggGame::computeAggregations(
    const int myRole,
    const AggregationInputMetrics& inputData) {
  XLOG(INFO, "Running private aggregation");

  std::vector<int64_t> ids = inputData.getIds();
  uint32_t numIds = ids.size();
  XLOGF(INFO, "Have {} ids", numIds);

  const int ciphertextSize = FLAGS_ciphertext_size;
  const int maxTouchpoints = FLAGS_max_num_touchpoints;
  const int maxConversions = FLAGS_max_num_conversions;

  // final output is (breakdown_id, aggregate)
  std::unordered_map<uint64_t, uint64_t> out;

  if (myRole == common::PARTNER) {
    // 0) Generate private key, public key and decryption table
    auto sk = heschme::PrivateKey::generate();
    auto pk = sk.toPublicKey();

    heschme::initializeElGamalDecryptionTable(FLAGS_decryption_table_size);

    // 1) Encrypt the attr values
    XLOG(INFO, "Encrypting partner conv values...");
    std::vector<uint8_t> ciphertextArray =
        encryptAttrResult(pk, inputData, maxTouchpoints, maxConversions);
    XLOGF(INFO, "Ciphertext array size  = {}", ciphertextArray.size());

    // 2) Send the ciphertext
    auto communicationAgent = communicationAgentFactory_->create(
        common::PUBLISHER, "he_aggregator_partner");
    communicationAgent->sendT(ciphertextArray);

    // 7) Receive number of groups and aggregated ciphertext
    // Receive num of groups
    XLOG(INFO, "Waiting to receive number of groups ... ");
    int msgSize = 1;
    std::vector<uint8_t> receivedNumGroups =
        communicationAgent->receive(msgSize);

    if (receivedNumGroups.size() == 0) {
      XLOG(ERR, "Received an empty array, cannot read number of groups");
      std::exit(1);
    };
    int numGroups = receivedNumGroups[0];
    XLOGF(INFO, "Received number of groups  = {}", numGroups);

    // Receive aggregated ciphertext
    XLOG(INFO, "Waiting to receive aggregated ciphertext ... ");
    msgSize = numGroups * ciphertextSize;
    std::vector<uint8_t> aggregatedCiphertexts =
        communicationAgent->receive(msgSize);
    XLOGF(INFO, "Received array size  = {}", aggregatedCiphertexts.size());

    // 8) Decrypt the aggregated ciphertext
    // Initialize a vector for decrypted plaintext
    std::vector<uint64_t> decryptedArray = decryptAggCiphertext(
        sk, aggregatedCiphertexts, numGroups, ciphertextSize);

    // 9) Send final decrypted result to publisher
    communicationAgent->sendT(decryptedArray);

  } else if (myRole == common::PUBLISHER) {
    // 3) Receive ciphertext from partner
    XLOG(INFO, "Starting to receive ciphertext...");
    int msgSize = numIds * maxTouchpoints * ciphertextSize;
    auto communicationAgent = communicationAgentFactory_->create(
        common::PARTNER, "he_aggregator_publisher");
    std::vector<uint8_t> ciphertextArray = communicationAgent->receive(msgSize);
    XLOGF(INFO, "Received array size  = {}", ciphertextArray.size());

    // 4) Aggregate ciphertext based on ad id
    XLOG(INFO, "Aggregating conv values...");
    std::unordered_map<uint64_t, heschme::Ciphertext> adIdToAggregate =
        aggregateCiphertexts(
            ciphertextArray,
            inputData,
            maxTouchpoints,
            maxConversions,
            ciphertextSize);

    // 5) Add noise to each ad_id bucket
    // Initialize noise generator and to be less than the size of the decryption
    // table
    std::random_device rd;
    std::mt19937_64 e(rd());
    std::uniform_int_distribution<int> randomInt(
        0, FLAGS_decryption_table_size - 1);
    std::vector<int> noiseVector;
    uint8_t numGroups = 0;
    std::vector<uint8_t> aggregatedCiphertexts;
    for (auto i = adIdToAggregate.begin(); i != adIdToAggregate.end(); i++) {
      // add noise
      int noise = randomInt(e);
      std::vector<uint8_t> c =
          heschme::Ciphertext::add_with_plaintext((i->second), noise).toBytes();
      aggregatedCiphertexts.insert(
          aggregatedCiphertexts.end(), c.begin(), c.end());

      // Keep track of the added noise
      noiseVector.push_back(std::move(noise));
      numGroups += 1;
    }

    // 6) Send the aggregated ciphertext to Partner
    std::vector<uint8_t> numGroupsArr{numGroups};
    communicationAgent->sendT(numGroupsArr);
    communicationAgent->sendT(aggregatedCiphertexts);

    // 10) Receive final result (Decrypted plaintext)
    XLOGF(INFO, "number of groups = {}", numGroups);
    msgSize = numGroups;

    auto receivedPlainTextArray =
        communicationAgent->receiveT<uint64_t>(msgSize);
    XLOGF(
        INFO,
        "Received receivedPlainTextArray size  = {}",
        receivedPlainTextArray.size());

    // 11) Remove the noise and generate output
    // Sanity checks
    if (noiseVector.size() != numGroups &&
        receivedPlainTextArray.size() != numGroups) {
      XLOG(
          ERR,
          "Noise vector and plaintext array has to be equal to the number of groups");
      std::exit(1);
    }

    int index = 0;
    for (auto i = adIdToAggregate.begin(); i != adIdToAggregate.end(); i++) {
      // Join the adId with the plaintext and noise
      int plainTextAgg = receivedPlainTextArray[index];
      int addedNoise = noiseVector[index];
      out[i->first] = plainTextAgg - addedNoise;
      XLOGF(
          INFO,
          "Index= {}, Adid = {}, Aggregate  = {}",
          index,
          i->first,
          plainTextAgg - addedNoise);
      ++index;
    }
  }

  return out;
}

} // namespace pcf2_he
