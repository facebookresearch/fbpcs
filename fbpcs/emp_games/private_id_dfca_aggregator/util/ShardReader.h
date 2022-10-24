/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <fbpcf/io/api/BufferedReader.h>
#include <folly/String.h>
#include <deque>
#include <queue>
#include <string>
#include <vector>

namespace private_id_dfca_aggregator {

class ShardReader {
 public:
  explicit ShardReader(
      std::unique_ptr<fbpcf::io::BufferedReader> bufferedReader)
      : bufferedReader_(std::move(bufferedReader)) {}

  std::string getNextChunk(size_t chunkSize) {
    std::string chunk;
    while (!isFinished() && chunk.size() + peekNextLine().size() < chunkSize) {
      chunk += readNextLine();
    }

    return chunk + std::string(chunkSize - chunk.size(), '\x00');
  }

  std::string peekNextLine() {
    if (lineBuffer_ == "") {
      lineBuffer_ = readNextLine();
    }
    return lineBuffer_;
  }

  std::string readNextLine() {
    if (lineBuffer_ != "") {
      auto tmp = lineBuffer_;
      lineBuffer_ = "";
      return tmp;
    } else {
      if (bufferedReader_->eof()) {
        return "";
      } else {
        auto line = bufferedReader_->readLine();
        folly::StringPiece privateId, userId;
        folly::split(",", line, privateId, userId);

        // Discard the header and unmatched PID results
        if (privateId.toString() == "id_" || userId.toString() == "0") {
          return readNextLine();
        } else {
          return privateId.toString() + "," + userId.toString() + "\n";
        }
      }
    }
  }

  bool isFinished() {
    return bufferedReader_->eof() && lineBuffer_ == "";
  }

 private:
  std::unique_ptr<fbpcf::io::BufferedReader> bufferedReader_;
  std::string lineBuffer_ = "";
};

} // namespace private_id_dfca_aggregator
