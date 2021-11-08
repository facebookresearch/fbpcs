/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <stdexcept>
#include <string>
#include <vector>

#include "fbpcs/emp_games/lift/common/DataFrame.h"

namespace df {
namespace detail {
std::vector<std::string> split(const std::string &s);
} // namespace detail

/**
 * Represents an error that a parsed row has a different length than the header.
 */
class RowLengthMismatch : public std::exception {
public:
  /**
   * Construct a new RowLengthMismatch error indicating that a parsed row has
   * a different length than the previously parsed header.
   *
   * @param headerSize the number of elements in the header
   * @param rowSize the number of elements in the newly parsed row
   */
  RowLengthMismatch(std::size_t headerSize, std::size_t rowSize) {
    msg_ = "Header has size " + std::to_string(headerSize) +
           " while row has size " + std::to_string(rowSize);
  }

  const char *what() const noexcept override { return msg_.c_str(); }

private:
  std::string msg_;
};

class CsvFileReadException : public std::exception {
public:
  explicit CsvFileReadException(const std::string &filePath) {
    msg_ = "Failed to read file '" + filePath + "'";
  }

  const char *what() const noexcept override { return msg_.c_str(); }

private:
  std::string msg_;
};

class CsvReader {
public:
  explicit CsvReader(const std::string &filePath);

  const std::vector<std::string> &getHeader() const { return header_; }

  std::vector<std::string> &getHeader() {
    return const_cast<std::vector<std::string> &>(
        const_cast<const CsvReader &>(*this).getHeader());
  }

  const std::vector<std::vector<std::string>> &getRows() const { return rows_; }

  std::vector<std::vector<std::string>> &getRows() {
    return const_cast<std::vector<std::vector<std::string>> &>(
        const_cast<const CsvReader &>(*this).getRows());
  }

private:
  std::vector<std::string> header_;
  std::vector<std::vector<std::string>> rows_;
};
} // namespace df
