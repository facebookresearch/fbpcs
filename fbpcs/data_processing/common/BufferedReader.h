/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <fbpcf/io/IFileManager.h>

#include <string>

constexpr int64_t kS3BufSize = 4096;

class BufferedReader {
 public:
  BufferedReader(
      std::unique_ptr<fbpcf::IFileManager> fileManager_,
      const std::string& filename)
      : fileManager_{std::move(fileManager_)}, filename_{filename} {}
  std::string readLine();
  std::size_t getBufLen() const {
    return bufLen_;
  }
  bool eof() const {
    return eof_;
  }

 private:
  std::size_t loadNextChunk();
  char consumeNextChar();

  bool everReadData_ = false;
  bool eof_ = false;
  std::unique_ptr<fbpcf::IFileManager> fileManager_;
  std::array<char, kS3BufSize> buffer_;
  std::size_t bufIdx_ = 0;
  std::size_t bufLen_ = 0;
  std::size_t nextRangeStart_ = 0;
  const std::string filename_;
};
