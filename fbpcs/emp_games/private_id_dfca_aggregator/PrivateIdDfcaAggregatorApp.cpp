/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <fbpcf/engine/communication/SocketPartyCommunicationAgentFactory.h>
#include <fbpcf/exception/exceptions.h>
#include <fbpcf/io/api/FileReader.h>
#include <folly/logging/xlog.h>
#include <memory>
#include <vector>

#include "fbpcs/emp_games/common/Constants.h"
#include "fbpcs/emp_games/private_id_dfca_aggregator/PrivateIdDfcaAggregatorApp.h"
#include "fbpcs/emp_games/private_id_dfca_aggregator/util/ShardReader.h"
#include "fbpcs/emp_games/private_id_dfca_aggregator/util/SortedIdSwapper.h"

namespace private_id_dfca_aggregator {

const size_t MSG_SIZE(BUFSIZ);
const std::vector<unsigned char> MSG_TERM(MSG_SIZE);

PrivateIdDfcaAggregatorApp::PrivateIdDfcaAggregatorApp(
    std::unique_ptr<
        fbpcf::engine::communication::IPartyCommunicationAgentFactory>
        communicationAgentFactory)
    : communicationAgentFactory_(std::move(communicationAgentFactory)) {}

void PrivateIdDfcaAggregatorApp::run(
    const std::int8_t party,
    const std::string& inputPath,
    const std::string& outputPath) {
  auto reader = std::make_unique<fbpcf::io::FileReader>(inputPath);
  auto bufferedReader =
      std::make_unique<fbpcf::io::BufferedReader>(std::move(reader));

  XLOG(INFO) << "Created reader for shard: " << inputPath;

  shardReader_ = std::make_shared<ShardReader>(std::move(bufferedReader));

  switch (party) {
    case common::PUBLISHER:
      runPublisher(outputPath);
      break;
    case common::PARTNER:
      runPartner();
      break;
    default:
      throw common::exceptions::NotImplementedError(
          "Party ID " + std::to_string(party) + " not supported.");
  }
}

void PrivateIdDfcaAggregatorApp::runPublisher(const std::string& outputPath) {
  auto sortedIdSwapper =
      std::make_unique<SortedIdSwapper>(shardReader_, outputPath);

  auto communicationAgent = communicationAgentFactory_->create(
      common::PARTNER, "pid_dfca_aggregator_publisher");

  auto partnerData = communicationAgent->receive(MSG_SIZE);
  while (partnerData != MSG_TERM && !shardReader_->isFinished()) {
    XLOG(INFO) << "Publisher: Received partner message -- size: "
               << partnerData.size();

    std::vector<char> buf(partnerData.begin(), partnerData.end());
    std::vector<std::string> partnerLines;
    folly::split('\n', buf, partnerLines);

    sortedIdSwapper->run(partnerLines);

    partnerData = communicationAgent->receive(MSG_SIZE);
  }

  XLOG(INFO) << "Publisher: Finished";
  sortedIdSwapper->close();
}

void PrivateIdDfcaAggregatorApp::runPartner() {
  auto communicationAgent = communicationAgentFactory_->create(
      common::PUBLISHER, "pid_dfca_aggregator_partner");

  while (!shardReader_->isFinished()) {
    auto message = shardReader_->getNextChunk(MSG_SIZE);

    const std::vector<unsigned char> data(message.begin(), message.end());

    XLOG(INFO) << "Partner: Sending message -- size: " << data.size();

    communicationAgent->send(data);
  }

  XLOG(INFO) << "Partner: Finished";
  communicationAgent->send(MSG_TERM);
}

} // namespace private_id_dfca_aggregator
