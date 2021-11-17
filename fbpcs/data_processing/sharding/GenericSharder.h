/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <string>
#include <vector>

namespace data_processing::sharder {
namespace detail {
/**
 * Remove quotes from a string in place. Example: "abc" -> abc
 *
 * @param s the string from which to remove quote characters
 */
void stripQuotes(std::string &s);
} // namespace detail

/**
 * A class which can shard data from one file into many sub-files.
 */
class GenericSharder {
public:
  /**
   * Create a new GenericSharder from the given input path and output paths.
   * Caller is responsible for generating output paths.
   *
   * @param inputPath a path to the file to be sharded
   * @param outputPaths a list of paths to the output sharded files
   * @param logEveryN how often to log progress updates
   */
  GenericSharder(std::string inputPath, std::vector<std::string> outputPaths,
                 int32_t logEveryN)
      : inputPath_{std::move(inputPath)}, outputPaths_{std::move(outputPaths)},
        logEveryN_{logEveryN} {}

  /**
   * Create a new GenericSharder from the given input path and output basepath.
   * This class is responsible for generating the exact output paths.
   *
   * @param inputPath a path to the file to be sharded
   * @param outputBasePath the prefix to use for all paths
   * @param startIndex the first subPath index to generate
   * @param endIndex the first subPath index to *not* generate (see gen)
   * @param logEveryN how often to log progress updates
   * @see GenericSharder::genOutputPaths
   */
  GenericSharder(std::string inputPath, std::string outputBasePath,
                 std::size_t startIndex, std::size_t endIndex,
                 int32_t logEveryN)
      : GenericSharder{std::move(inputPath),
                       genOutputPaths(outputBasePath, startIndex, endIndex),
                       logEveryN} {}

  /**
   * Virtual destructor because C++.
   */
  virtual ~GenericSharder() {}

  /**
   * Generate output paths from a base path and a set of indices. For a basepath
   * `/foo` and start=0,end=4, we generate /foo_0, /foo_1, /foo_2, and /foo_3.
   * In other words, we have a half-open interval [startIndex, endIndex).
   *
   * @param outputBasePath the prefix to use for all paths
   * @param startIndex the first subPath index to generate
   * @param endIndex the first subPath index to *not* generate
   * @returns a vector of strings representing generated output paths where the
   *     output will be written
   */
  static std::vector<std::string>
  genOutputPaths(const std::string &outputBasePath, std::size_t startIndex,
                 std::size_t endIndex);

  /**
   * Get a reference to this sharder's input path.
   *
   * @returns a reference to this sharder's input path
   */
  const std::string &getInputPath() const { return inputPath_; }

  /**
   * Get a reference to this sharder's output paths
   *
   * @returns a reference to this sharder's output paths
   */
  const std::vector<std::string> &getOutputPaths() const {
    return outputPaths_;
  }

  /**
   * Retrieve how often this sharder should log progress updates.
   *
   * @returns how often this sharder should log progress updates
   */
  int32_t getLogRate() const { return logEveryN_; }

  /**
   * Run the sharder.
   */
  virtual void shard() const = 0;

private:
  std::string inputPath_;
  std::vector<std::string> outputPaths_;
  int32_t logEveryN_;
};
} // namespace data_processing::sharder
