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
#include "fbpcs/emp_games/private_id_dfca_aggregator/util/KWayShardsMerger.h"
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
    const std::int32_t numShards,
    const std::int32_t shardStartIndex,
    const std::string& inputPath,
    const std::string& inputFilePrefix,
    const std::string& outputPath) {
  std::vector<std::shared_ptr<fbpcf::io::BufferedReader>> shardReaders;

  for (int i = shardStartIndex; i < shardStartIndex + numShards; i++) {
    std::string fullPath =
        folly::sformat("{}/{}_{}", inputPath, inputFilePrefix, i);
    auto reader = std::make_unique<fbpcf::io::FileReader>(fullPath);
    auto bufferedReader =
        std::make_shared<fbpcf::io::BufferedReader>(std::move(reader));
    shardReaders.push_back(bufferedReader);

    XLOG(INFO) << "Created reader for shard: " << fullPath;
  }

  kWayShardsMerger_ =
      std::make_unique<KWayShardsMerger>(std::move(shardReaders));

  auto sortedIdSwapper = std::make_unique<SortedIdSwapper>(outputPath);

  switch (party) {
    case common::PUBLISHER:
      runPublisher(std::move(sortedIdSwapper));
      break;
    case common::PARTNER:
      runPartner();
      break;
    default:
      throw common::exceptions::NotImplementedError(
          "Party ID " + std::to_string(party) + " not supported.");
  }
}

void PrivateIdDfcaAggregatorApp::runPublisher(
    std::unique_ptr<SortedIdSwapper> sortedIdSwapper) {
  auto commAgent =
      communicationAgentFactory_->create(common::PARTNER, "publisher_partner");

  auto partnerData = commAgent->receive(MSG_SIZE);
  while (partnerData != MSG_TERM && !kWayShardsMerger_->isFinished()) {
    XLOG(INFO) << "Publisher: Received partner message -- size: "
               << partnerData.size();

    std::vector<char> buf(partnerData.begin(), partnerData.end());
    std::vector<std::string> partnerLines;
    folly::split("\n", buf, partnerLines);

    sortedIdSwapper->run(std::move(kWayShardsMerger_), partnerLines);

    partnerData = commAgent->receive(MSG_SIZE);
  }

  XLOG(INFO) << "Publisher: Finished";
  sortedIdSwapper->close();
}

void PrivateIdDfcaAggregatorApp::runPartner() {
  auto commAgent = communicationAgentFactory_->create(
      common::PUBLISHER, "partner_publisher");

  while (!kWayShardsMerger_->isFinished()) {
    auto message = kWayShardsMerger_->getNextChunk(MSG_SIZE);

    const std::vector<unsigned char> data(message.begin(), message.end());

    XLOG(INFO) << "Partner: Sending message -- size: " << data.size();
    commAgent->send(data);
  }

  XLOG(INFO) << "Partner: Finished";
  commAgent->send(MSG_TERM);
}

} // namespace private_id_dfca_aggregator
