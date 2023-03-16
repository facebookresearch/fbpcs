/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <boost/archive/text_iarchive.hpp>
#include <boost/archive/text_oarchive.hpp>
#include <boost/serialization/vector.hpp>
#include <fbpcf/io/api/BufferedReader.h>
#include <fbpcf/io/api/BufferedWriter.h>
#include <stdint.h>
#include <fstream>
#include <stdexcept>

#include "fbpcf/mpc_std_lib/unified_data_process/data_processor/UdpDecryption.h"
#include "fbpcf/mpc_std_lib/unified_data_process/data_processor/UdpUtil.h"
#include "fbpcs/emp_games/data_processing/global_parameters/GlobalParameters.h"

namespace unified_data_process {

template <int schedulerId>
class UdpDecryptorApp {
  using Decryption =
      fbpcf::mpc_std_lib::unified_data_process::data_processor::UdpDecryption<
          schedulerId>;

  using SecString = fbpcf::frontend::BitString<true, schedulerId, true>;

 public:
  UdpDecryptorApp(std::unique_ptr<Decryption> decryption, bool amIPublisher)
      : decryption_(std::move(decryption)), amIPublisher_(amIPublisher) {}

  std::tuple<SecString, SecString> invokeUdpDecryption(
      const std::string& dataFile,
      const std::string& expandedKeyFile,
      const std::string& globalParameterFile) const {
    auto gp = global_parameters::readFromFile(globalParameterFile);
    auto publisherWidth =
        boost::get<int32_t>(gp.at(global_parameters::KPubDataWidth));
    auto advertiserWidth =
        boost::get<int32_t>(gp.at(global_parameters::KAdvDataWidth));
    auto intersectionSize =
        boost::get<int32_t>(gp.at(global_parameters::KMatchedUserCount));

    if (amIPublisher_) {
      auto myData = decryption_->decryptMyData(
          fbpcf::mpc_std_lib::unified_data_process::data_processor::
              readExpandedKeyFromFile(expandedKeyFile),
          publisherWidth,
          intersectionSize);
      auto encryptionResults = fbpcf::mpc_std_lib::unified_data_process::
          data_processor::readEncryptionResultsFromFile(dataFile);
      auto peerData = decryption_->decryptPeerData(
          encryptionResults.ciphertexts,
          encryptionResults.nonces,
          encryptionResults.indexes);
      return {myData, peerData};
    } else {
      auto encryptionResults = fbpcf::mpc_std_lib::unified_data_process::
          data_processor::readEncryptionResultsFromFile(dataFile);
      auto peerData = decryption_->decryptPeerData(
          encryptionResults.ciphertexts,
          encryptionResults.nonces,
          encryptionResults.indexes);
      auto myData = decryption_->decryptMyData(
          fbpcf::mpc_std_lib::unified_data_process::data_processor::
              readExpandedKeyFromFile(expandedKeyFile),
          advertiserWidth,
          intersectionSize);
      return {peerData, myData};
    }
  }

 private:
  std::unique_ptr<Decryption> decryption_;
  bool amIPublisher_;
};

} // namespace unified_data_process
