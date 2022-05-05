/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "fbpcs/data_processing/sharding/GenericSharder.h"

#include <algorithm>
#include <exception>
#include <filesystem>
#include <fstream>
#include <memory>
#include <sstream>
#include <stdexcept>
#include <string>
#include <vector>

#include <fbpcf/io/FileManagerUtil.h>
#include <folly/Random.h>
#include <folly/executors/CPUThreadPoolExecutor.h>
#include <folly/logging/xlog.h>
#include "fbpcf/io/api/BufferedReader.h"
#include "fbpcf/io/api/FileReader.h"

#include "fbpcs/data_processing/common/FilepathHelpers.h"
#include "fbpcs/data_processing/common/Logging.h"
#include "fbpcs/data_processing/common/S3CopyFromLocalUtil.h"
#include "folly/String.h"

namespace data_processing::sharder {
namespace detail {
void stripQuotes(std::string& s) {
  s.erase(std::remove(s.begin(), s.end(), '"'), s.end());
}

void dos2Unix(std::string& s) {
  s.erase(std::remove(s.begin(), s.end(), '\r'), s.end());
}

void strRemoveBlanks(std::string& str) {
  str.erase(std::remove(str.begin(), str.end(), ' '), str.end());
}
} // namespace detail

static const std::string kIdColumnPrefix = "id_";

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
  auto reader = fbpcf::io::FileReader(getInputPath());
  auto bufferedReader =
      std::make_unique<fbpcf::io::BufferedReader>(reader, BUFFER_SIZE);

  std::filesystem::path tmpDirectory{"/tmp"};
  std::vector<std::string> tmpFilenames;
  std::vector<std::unique_ptr<std::ofstream>> tmpFiles;

  auto filename = std::filesystem::path{
      private_lift::filepath_helpers::getBaseFilename(getInputPath())};
  auto stem = filename.stem().string();
  auto extension = filename.extension().string();
  // Get a random ID to avoid potential name collisions if multiple
  // runs at the same time point to the same input file
  auto randomId = std::to_string(folly::Random::secureRand64());

  for (auto i = 0; i < numShards; ++i) {
    std::stringstream tmpName;
    tmpName << randomId << "_" << stem << "_" << i << extension;

    auto tmpFilepath = tmpDirectory / tmpName.str();

    tmpFilenames.push_back(tmpFilepath.string());
    tmpFiles.push_back(std::make_unique<std::ofstream>(tmpFilepath));
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

  for (const auto& tmpFile : tmpFiles) {
    *tmpFile << line << "\n";
  }
  XLOG(INFO) << "Got header line: '" << line << "'";

  // Read lines and send to appropriate outFile repeatedly
  uint64_t lineIdx = 0;
  while (!bufferedReader->eof()) {
    line = bufferedReader->readLine();
    detail::stripQuotes(line);
    detail::dos2Unix(line);
    detail::strRemoveBlanks(line);
    shardLine(std::move(line), tmpFiles, idColumnIndices);
    ++lineIdx;
    if (lineIdx % getLogRate() == 0) {
      XLOG(INFO) << "Processed line "
                 << private_lift::logging::formatNumber(lineIdx);
    }
  }

  XLOG(INFO) << "Finished after processing "
             << private_lift::logging::formatNumber(lineIdx) << " lines.";

  XLOG(INFO) << "Now copying files to final output path...";

  auto executor =
      std::make_unique<folly::CPUThreadPoolExecutor>(THREAD_POOL_SIZE);
  std::vector<std::exception_ptr> errorStorage(numShards, nullptr);

  bufferedReader->close();

  for (auto i = 0; i < numShards; ++i) {
    auto outputDst = getOutputPaths().at(i);
    auto tmpFileSrc = tmpFilenames.at(i);

    if (outputDst == tmpFileSrc) {
      continue;
    }

    // Reset underlying unique_ptr to ensure buffer gets flushed
    tmpFiles.at(i).reset();

    executor->add([this, outputDst, tmpFileSrc, &errorStorage, i] {
      copySingleFileToDestination(outputDst, tmpFileSrc, errorStorage, i);
    });
    XLOG(INFO, fmt::format("Shard {} has {} rows", i, rowsInShard[i]));
  }
  executor->join();

  // check for errors from worker threads
  auto numFailedTasks = 0;
  std::exception_ptr exceptionToThrow = nullptr;
  for (int i = 0; i < numShards; i++) {
    if (errorStorage.at(i) != nullptr) {
      numFailedTasks++;
      exceptionToThrow = errorStorage.at(i);
    }
  }
  if (numFailedTasks == 0) {
    XLOG(INFO) << "All file writes successful";
  } else {
    XLOG(INFO) << "There was a failure on " << numFailedTasks << " threads.";
    std::rethrow_exception(exceptionToThrow);
  }
}

void GenericSharder::shardLine(
    std::string line,
    const std::vector<std::unique_ptr<std::ofstream>>& outFiles,
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
  *outFiles.at(shard) << line << "\n";
}

void GenericSharder::copySingleFileToDestination(
    std::string outputDst,
    std::string tmpFileSrc,
    std::vector<std::exception_ptr>& errorStorage,
    int i) {
  try {
    copySingleFileToDestinationImpl(outputDst, tmpFileSrc);
  } catch (...) {
    errorStorage.at(i) = std::current_exception();
  }
}

void GenericSharder::copySingleFileToDestinationImpl(
    std::string outputDst,
    std::string tmpFileSrc) {
  XLOG(INFO) << "Writing " << tmpFileSrc << " -> " << outputDst;
  auto outputType = fbpcf::io::getFileType(outputDst);
  if (outputType == fbpcf::io::FileType::S3) {
    private_lift::s3_utils::uploadToS3(tmpFileSrc, outputDst);
  } else if (outputType == fbpcf::io::FileType::Local) {
    std::filesystem::copy(
        tmpFileSrc,
        outputDst,
        std::filesystem::copy_options::overwrite_existing);
  } else {
    throw std::runtime_error{"Unsupported output destination"};
  }

  // We need to make sure we clean up the tmpfiles now
  std::remove(tmpFileSrc.c_str());
}
} // namespace data_processing::sharder
