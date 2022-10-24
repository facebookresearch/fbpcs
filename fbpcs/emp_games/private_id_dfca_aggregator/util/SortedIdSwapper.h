/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <fbpcf/io/api/BufferedWriter.h>
#include <fbpcf/io/api/FileWriter.h>
#include <folly/String.h>
#include <folly/logging/xlog.h>
#include <memory>
#include <string>
#include <vector>

#include "fbpcs/emp_games/private_id_dfca_aggregator/util/ShardReader.h"

namespace private_id_dfca_aggregator {

class SortedIdSwapper {
 public:
  explicit SortedIdSwapper(
      std::shared_ptr<ShardReader> publisherShardReader,
      const std::string& outputPath) {
    publisherShardReader_ = publisherShardReader;

    auto writer = std::make_unique<fbpcf::io::FileWriter>(outputPath);
    bufferedWriter_ =
        std::make_unique<fbpcf::io::BufferedWriter>(std::move(writer));

    XLOG(INFO) << "Initializing output with header line at " << outputPath;

    bufferedWriter_->writeString("publisher_user_id,partner_user_id\n");
  }

  void run(std::vector<std::string> partnerLines) {
    int partnerIdx = 0;

    while (partnerIdx < partnerLines.size() &&
           !publisherShardReader_->isFinished()) {
      auto publisherLine = publisherShardReader_->peekNextLine();
      folly::StringPiece publisherPrivateId, publisherUserId;
      folly::split(",", publisherLine, publisherPrivateId, publisherUserId);

      folly::StringPiece partnerPrivateId, partnerUserId;
      folly::split(
          ",", partnerLines[partnerIdx], partnerPrivateId, partnerUserId);

      auto compareRes = publisherPrivateId.compare(partnerPrivateId.toString());

      if (compareRes == 0) { // match

        std::vector<std::string> userIds{
            folly::trimWhitespace(publisherUserId).toString(),
            folly::trimWhitespace(partnerUserId).toString()};

        auto newLine = folly::join(",", userIds) + "\n";
        bufferedWriter_->writeString(newLine);

        partnerIdx++;
        publisherLine = publisherShardReader_->readNextLine();

      } else if (compareRes < 0) { // publisherPrivateId < partnerPrivateId

        publisherLine = publisherShardReader_->readNextLine();

      } else if (compareRes > 0) { // publisherPrivateId > partnerPrivateId

        partnerIdx++;
      }
    }
  }

  void close() {
    bufferedWriter_->close();
  }

 private:
  std::unique_ptr<fbpcf::io::BufferedWriter> bufferedWriter_;
  std::shared_ptr<ShardReader> publisherShardReader_;
};

} // namespace private_id_dfca_aggregator
