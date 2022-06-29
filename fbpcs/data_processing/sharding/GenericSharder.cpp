/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "fbpcs/data_processing/sharding/GenericSharder.h"

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <exception>
#include <filesystem>
#include <fstream>
#include <memory>
#include <sstream>
#include <stdexcept>
#include <string>
#include <vector>

#include <fbpcf/aws/S3Util.h>
#include <fbpcf/io/FileManagerUtil.h>
#include <fbpcf/io/api/BufferedReader.h>
#include <fbpcf/io/api/BufferedWriter.h>
#include <fbpcf/io/api/FileReader.h>
#include <fbpcf/io/api/FileWriter.h>
#include <folly/Random.h>
#include <folly/executors/CPUThreadPoolExecutor.h>
#include <folly/logging/xlog.h>
#include <re2/re2.h>

#include <folly/DynamicConverter.h>
#include <folly/json.h>
#include "fbpcs/data_processing/common/FilepathHelpers.h"
#include "fbpcs/data_processing/common/Logging.h"
#include "fbpcs/data_processing/common/S3CopyFromLocalUtil.h"
#include "folly/String.h"

namespace data_processing::sharder {
namespace detail {
void stripQuotes(std::string& s) {
  s.erase(std::remove(s.begin(), s.end(), '"'), s.end());
  s.erase(std::remove(s.begin(), s.end(), '\''), s.end());
}

void dos2Unix(std::string& s) {
  s.erase(std::remove(s.begin(), s.end(), '\r'), s.end());
}

void strRemoveBlanks(std::string& str) {
  str.erase(std::remove(str.begin(), str.end(), ' '), str.end());
}

void strReplaceNullColumnWithEmpty(std::string& str) {
  std::string tmp = "";
  re2::RE2 Regex("(^|,)(?i)null($|,)");
  while (tmp != str) {
    // in case there are overlapping matches, it would iterate to replace all
    // the null columns. e.g. null,null,null --> ,null, --> ,,
    tmp = str;
    re2::RE2::Replace(&str, Regex, "\\1\\2");
  }
}
} // namespace detail

static const std::string kIdColumnPrefix = "id_";

/*
  The chunk size for writing to cloud storage (currently
  only AWS S3) must be greater than 5 MB, per the AWS
  documentation. Otherwise multipart upload will fail.

  The number below is 5 MB in bytes.
*/
static const uint64_t kBufferedWriterChunkSize = 5'242'880;

std::vector<std::string> GenericSharder::genOutputPaths(
    const std::string& outputBasePath,
    std::size_t startIndex,
    std::size_t endIndex) {
  std::vector<std::string> res;
  for (std::size_t i = startIndex; i < endIndex; ++i) {
    res.push_back(outputBasePath + '_' + std::to_string(i));
  }
  return res;
}

void GenericSharder::shard() {
  std::size_t numShards = getOutputPaths().size();
  auto reader = std::make_unique<fbpcf::io::FileReader>(getInputPath());
  auto bufferedReader = std::make_unique<fbpcf::io::BufferedReader>(
      std::move(reader), BUFFER_SIZE);

  std::vector<std::unique_ptr<fbpcf::io::BufferedWriter>> outFiles(0);

  for (std::size_t i = 0; i < numShards; ++i) {
    auto fileWriter =
        std::make_unique<fbpcf::io::FileWriter>(getOutputPaths().at(i));
    auto bufferedWriter = std::make_unique<fbpcf::io::BufferedWriter>(
        std::move(fileWriter), kBufferedWriterChunkSize);
    outFiles.push_back(std::move(bufferedWriter));
    XLOG(INFO) << "Created buffered writer for shard " << std::to_string(i);
  }
  // First get the header and put it in all the output files
  std::string line = bufferedReader->readLine();
  detail::stripQuotes(line);
  detail::dos2Unix(line);
  detail::strRemoveBlanks(line);

  std::vector<std::string> header;
  folly::split(",", line, header);

  // find indices of columns with its column name start with kIdColumnPrefix
  std::vector<int32_t> idColumnIndices;
  for (int idx = 0; idx < header.size(); idx++) {
    if (header[idx].compare(0, kIdColumnPrefix.length(), kIdColumnPrefix) ==
        0) {
      idColumnIndices.push_back(idx);
    }
  }
  if (0 == idColumnIndices.size()) {
    // note: it's not *essential* to clean up tmpfile here, but it will
    // pollute our test directory otherwise, which is just somewhat annoying.
    XLOG(FATAL) << kIdColumnPrefix
                << " prefixed-column missing from input header"
                << "Header: [" << folly::join(",", header) << "]";
  }

  std::string newLine = "\n";
  std::size_t i = 0;
  for (const auto& outFile : outFiles) {
    XLOG(INFO) << "Writing header to shard " << std::to_string(i++);
    outFile->writeString(line);
    outFile->writeString(newLine);
  }
  XLOG(INFO) << "Got header line: '" << line << "'";

  // Read lines and send to appropriate outFile repeatedly
  uint64_t lineIdx = 0;
  while (!bufferedReader->eof()) {
    line = bufferedReader->readLine();
    detail::stripQuotes(line);
    detail::dos2Unix(line);
    detail::strRemoveBlanks(line);
    detail::strReplaceNullColumnWithEmpty(line);
    shardLine(std::move(line), outFiles, idColumnIndices);
    ++lineIdx;
    if (lineIdx % getLogRate() == 0) {
      XLOG(INFO) << "Processed line "
                 << private_lift::logging::formatNumber(lineIdx);
    }
  }

  // Log number of rows in each shard to the
  // "<filepath_for_0th_shard>_shardDistribution" file.
  logShardDistribution();

  XLOG(INFO) << "Finished after processing "
             << private_lift::logging::formatNumber(lineIdx) << " lines.";

  bufferedReader->close();

  for (auto i = 0; i < numShards; ++i) {
    outFiles.at(i)->close();
    XLOG(INFO, fmt::format("Shard {} has {} rows", i, getRowsForShard(i)));
  }

  XLOG(INFO) << "All file writes successful";
}

void GenericSharder::shardLine(
    std::string line,
    const std::vector<std::unique_ptr<fbpcf::io::BufferedWriter>>& outFiles,
    const std::vector<int32_t>& idColumnIndices) {
  std::vector<std::string> cols;
  folly::split(",", line, cols);

  std::string id = "";
  for (auto idColumnIdx : idColumnIndices) {
    if (idColumnIdx >= cols.size()) {
      XLOG_EVERY_MS(INFO, 5000)
          << "Discrepancy with header:" << line << " does not have "
          << idColumnIdx << "th column.\n";
      return;
    }
    id = cols.at(idColumnIdx);
    if (!id.empty()) {
      break;
    }
  }
  if (id.empty()) {
    XLOG_EVERY_MS(INFO, 5000) << "All the id values are empty in this row";
    return;
  }
  auto shard = getShardFor(id, outFiles.size());
  logRowsToShard(shard);
  std::string newLine = "\n";
  outFiles.at(shard)->writeString(line);
  outFiles.at(shard)->writeString(newLine);
}

void GenericSharder::logShardDistribution() {
  std::size_t numShards = getOutputPaths().size();
  if (numShards == 0) {
    XLOG(INFO) << "No shard to write distribution to.";
    return;
  }
  std::string outputPath = getOutputPaths().at(0);
  if (outputPath.empty()) {
    XLOG(INFO) << "No filepath present to log shard distribution to.";
    return;
  }
  const std::string& shardDistributionPath =
      outputPath + '_' + "shardDistribution";
  auto fWriter = std::make_unique<fbpcf::io::FileWriter>(shardDistributionPath);
  auto bWriter = std::make_unique<fbpcf::io::BufferedWriter>(
      std::move(fWriter), kBufferedWriterChunkSize);
  std::string shardDstributionJson = getShardDistributionJson();
  bWriter->writeString(shardDstributionJson);
  bWriter->close();
  XLOG(INFO) << "Distribution of shards written to: '" << shardDistributionPath
             << "'";
}

std::string GenericSharder::getShardDistributionJson() {
  auto rowsInShardDynamic = folly::toDynamic(rowsInShard);
  std::string shardDstributionStr = folly::toPrettyJson(rowsInShardDynamic);
  return shardDstributionStr;
}
} // namespace data_processing::sharder
