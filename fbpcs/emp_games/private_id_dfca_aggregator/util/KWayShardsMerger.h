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
#include <vector>

namespace private_id_dfca_aggregator {

struct ShardEntry {
  std::int32_t shardId;
  std::string privateId;
  std::string userId;
};

struct ShardEntryComparator {
  bool operator()(ShardEntry& t1, ShardEntry& t2) {
    return t1.privateId > t2.privateId;
  }
};

class KWayShardsMerger {
 public:
  explicit KWayShardsMerger(
      std::vector<std::shared_ptr<fbpcf::io::BufferedReader>> shardReaders)
      : shardReaders_(shardReaders) {
    fillQueue();
  }

  std::string getNextChunk(size_t chunkSize) {
    auto chunk = initChunk_;

    std::string nextLine;
    while (!isFinished() && chunk.size() < chunkSize) {
      nextLine = getNextLine();
      chunk += nextLine;
    }

    // If over chunk size, resize and save last line for next read
    if (chunk.size() > chunkSize) {
      initChunk_ = nextLine;
      chunk.resize(chunk.size() - nextLine.size());
    }

    return chunk + std::string(chunkSize - chunk.size(), '\x00');
  }

  std::string getNextLine() {
    if (privateIdMinQueue_.empty() || isFinished()) {
      return "";
    } else {
      auto shardEntry = privateIdMinQueue_.top();
      privateIdMinQueue_.pop();
      storeNextShardEntry(shardEntry.shardId);
      return shardEntry.privateId + "," + shardEntry.userId + "\n";
    }
  }

  bool isFinished() {
    return eofShards_ >= shardReaders_.size();
  }

 protected:
  void fillQueue() {
    int shardId = 0;
    while (!isFinished() && shardId < shardReaders_.size()) {
      storeNextShardEntry(shardId);
      shardId++;
    }
  }

  void storeNextShardEntry(int nextShardId) {
    if (!isFinished()) {
      if (!shardReaders_[nextShardId]->eof()) {
        auto line = shardReaders_[nextShardId]->readLine();
        folly::StringPiece privateId, userId;
        folly::split(",", line, privateId, userId);

        if (privateId.toString() == "id_") {
          storeNextShardEntry(nextShardId);
        } else {
          ShardEntry shardEntry;
          shardEntry.shardId = nextShardId;
          shardEntry.privateId = privateId.toString();
          shardEntry.userId = userId.toString();

          privateIdMinQueue_.push(shardEntry);
        }
      } else {
        eofShards_++;
        storeNextShardEntry((nextShardId + 1) % shardReaders_.size());
      }
    }
  }

 private:
  std::priority_queue<ShardEntry, std::deque<ShardEntry>, ShardEntryComparator>
      privateIdMinQueue_;
  std::vector<std::shared_ptr<fbpcf::io::BufferedReader>> shardReaders_;
  std::int32_t eofShards_ = 0;
  size_t chunkSize_;
  std::string initChunk_ = "";
};

} // namespace private_id_dfca_aggregator
