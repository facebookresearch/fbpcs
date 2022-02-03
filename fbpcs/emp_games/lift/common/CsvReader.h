/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
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
/**
 * Splits a string up into a vector of strings on comma characters. There is
 * support for "list" fields denoted by brackets `[]`. For example, the string
 * `abc,[1,2,3],4,5` would be split as `{"abc", "[1,2,3]", "4", "5"}`.
 *
 * @param s the string to split
 * @returns a vector of strings split on commas
 */
std::vector<std::string> split(const std::string& s);
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

  const char* what() const noexcept override {
    return msg_.c_str();
  }

 private:
  std::string msg_;
};

/**
 * An exception that is thrown if there is an error parsing a CSV file.
 */
class CsvFileReadException : public std::exception {
 public:
  /**
   * Constructs an exception referring to the given filePath.
   *
   * @param filePath the file that the CsvReader failed to parse
   */
  explicit CsvFileReadException(const std::string& filePath) {
    msg_ = "Failed to read file '" + filePath + "'";
  }

  const char* what() const noexcept override {
    return msg_.c_str();
  }

 private:
  std::string msg_;
};

/**
 * A class which parses a CSV file into a header and a vector of rows.
 */
class CsvReader {
 public:
  /**
   * Constructs a new CsvReader to read the given filePath.
   *
   * @param filePath the filePath of the CSV to be read
   */
  explicit CsvReader(const std::string& filePath);

  /**
   * Get the header parsed from the file passed in the constructor.
   *
   * @returns the header of the file as a vector of strings
   */
  const std::vector<std::string>& getHeader() const {
    return header_;
  }

  /**
   * Non-const version of `CsvReader::getHeader`. Get the header parsed from the
   * file passed in the constructor.
   *
   *
   * @returns the header of the file as a vector of strings
   */
  std::vector<std::string>& getHeader() {
    return const_cast<std::vector<std::string>&>(
        const_cast<const CsvReader&>(*this).getHeader());
  }

  /**
   * Get the rows parsed from the file passed in the constructor. This will not
   * include the header row (to get the header, use `CsvReader::getHeader`).
   *
   * @returns the rows of the file as a vector of vector strings
   */
  const std::vector<std::vector<std::string>>& getRows() const {
    return rows_;
  }

  /**
   * Non-const version of `CsvReader::getRows`. Get the rows parsed from the
   * file passed in the constructor. This will not include the header row (to
   * get the header, use `CsvReader::getHeader`).
   *
   * @returns the rows of the file as a vector of vector strings
   */
  std::vector<std::vector<std::string>>& getRows() {
    return const_cast<std::vector<std::vector<std::string>>&>(
        const_cast<const CsvReader&>(*this).getRows());
  }

 private:
  std::vector<std::string> header_;
  std::vector<std::vector<std::string>> rows_;
};
} // namespace df
