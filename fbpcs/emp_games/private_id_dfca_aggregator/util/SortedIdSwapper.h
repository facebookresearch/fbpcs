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
#include <string>
#include <vector>

#include "fbpcs/emp_games/private_id_dfca_aggregator/util/KWayShardsMerger.h"

namespace private_id_dfca_aggregator {

class SortedIdSwapper {
 public:
  explicit SortedIdSwapper(const std::string& outputPath) {
    auto writer = std::make_unique<fbpcf::io::FileWriter>(outputPath);
    bufferedWriter_ =
        std::make_unique<fbpcf::io::BufferedWriter>(std::move(writer));

    XLOG(INFO) << "Initializing output with header line at " << outputPath;

    bufferedWriter_->writeString("publisher_user_id,partner_user_id\n");
  }

  void run(
      std::unique_ptr<KWayShardsMerger> publisherKWayShardsMerger_,
      std::vector<std::string> partnerLines) {
    int partnerIdx = 0;

    auto publisherLine = publisherKWayShardsMerger_->getNextLine();
    while (partnerIdx < partnerLines.size() &&
           !publisherKWayShardsMerger_->isFinished()) {
      auto partnerLine = &partnerLines[partnerIdx];

      if (publisherLine == "") {
        publisherLine = publisherKWayShardsMerger_->getNextLine();
      } else if (*partnerLine == "") {
        partnerIdx++;
      } else {
        folly::StringPiece publisherPrivateId, publisherUserId;
        folly::split(",", publisherLine, publisherPrivateId, publisherUserId);

        folly::StringPiece partnerPrivateId, partnerUserId;
        folly::split(",", *partnerLine, partnerPrivateId, partnerUserId);

        auto compareRes =
            publisherPrivateId.compare(partnerPrivateId.toString());

        if (compareRes == 0) { // match

          std::vector<std::string> userIds{
              folly::trimWhitespace(publisherUserId).toString(),
              folly::trimWhitespace(partnerUserId).toString()};

          auto newLine = folly::join(",", userIds) + "\n";
          bufferedWriter_->writeString(newLine);

          partnerIdx++;
        } else if (compareRes < 0) { // publisherPrivateId < partnerPrivateId

          publisherLine = publisherKWayShardsMerger_->getNextLine();

        } else if (compareRes > 0) { // publisherPrivateId > partnerPrivateId

          partnerIdx++;
        }
      }
    }
  }

  void close() {
    bufferedWriter_->close();
  }

 private:
  std::unique_ptr<fbpcf::io::BufferedWriter> bufferedWriter_;
};

} // namespace private_id_dfca_aggregator
