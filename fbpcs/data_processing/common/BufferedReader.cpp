/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "BufferedReader.h"

#include "fbpcf/io/LocalFileManager.h"

#include <string>

std::string BufferedReader::readLine() {
  std::string res;
  bool foundEnd = false;
  // Read until we find a newline or EOF character
  // If we run out of buffer, try to read more
  // If that's empty, we're out of characters to read.
  while (!foundEnd) {
    // We're out of buffer, refill by reading the next range
    if (bufIdx_ >= getBufLen()) {
      if (loadNextChunk() == 0) {
        // if there was no next chunk to load, then we hit the end
        // of file
        eof_ = true;
        if (!everReadData_) {
          throw std::runtime_error{"Never read any data"};
        } else {
          // tried to load a new chunk, but we're out of data, therefore we're
          // at the end
          foundEnd = true;
          continue;
        }
      }
    }

    everReadData_ = true;
    char nextChar = consumeNextChar();
    if (nextChar == '\n' || nextChar == '\0') {
      foundEnd = true;
    } else {
      res += nextChar;
    }
  }

  return res;
}

std::size_t BufferedReader::loadNextChunk() {
  try {
    auto str = fileManager_->readBytes(
        filename_, nextRangeStart_, nextRangeStart_ + buffer_.size());
    nextRangeStart_ += str.size();
    bufIdx_ = 0;
    bufLen_ = str.size();
    str.copy(buffer_.data(), str.size());
    return str.size();
  } catch (...) {
    // We let the caller know that zero bytes were read
    // We need to catch the exception because it's *possible* we're just
    // at the end of the file and there are no more bytes to read.
    return 0;
  }
}

char BufferedReader::consumeNextChar() {
  if (bufIdx_ < getBufLen() && bufIdx_ < buffer_.size()) {
    char res = buffer_.at(bufIdx_);
    ++bufIdx_;
    return res;
  } else {
    throw std::runtime_error{
        "Unexpected error, tried to read character outside of buffer"};
  }
}
