/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "LiftIdSpineFileCombiner.h"

#include <cstdio>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <sstream>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <vector>

#include <folly/Random.h>
#include <folly/String.h>
#include <folly/logging/xlog.h>
#include <re2/re2.h>

// TODO: Rewrite for OSS?
#include "../common/FilepathHelpers.h"
#include "../common/S3CopyFromLocalUtil.h"
#include "../id_combiner/AddPaddingToCols.h"
#include "../id_combiner/DataPreparationHelpers.h"
#include "../id_combiner/DataValidation.h"
#include "../id_combiner/GroupBy.h"
#include "../id_combiner/IdSwapMultiKey.h"
#include "../id_combiner/SortIds.h"
#include "../id_combiner/SortIntegralValues.h"
#include "fbpcf/io/FileManagerUtil.h"
#include "fbpcf/io/IInputStream.h"
#include "fbpcf/io/api/BufferedReader.h"
#include "fbpcf/io/api/FileReader.h"
#include "fbpcs/data_processing/lift_id_combiner/LiftIdSpineCombinerOptions.h"

namespace pid {
void LiftIdSpineFileCombiner::combineFile() {
  auto dataReader = std::make_unique<fbpcf::io::FileReader>(dataPath_);
  auto spineReader = std::make_unique<fbpcf::io::FileReader>(spinePath_);
  auto bufferedDataReader = std::make_shared<fbpcf::io::BufferedReader>(
      std::move(dataReader), kBufferedReaderChunkSize);
  auto bufferedSpineReader = std::make_shared<fbpcf::io::BufferedReader>(
      std::move(spineReader), kBufferedReaderChunkSize);

  // Get a random ID to avoid potential name collisions if multiple
  // runs at the same time point to the same input file
  auto randomId = std::to_string(folly::Random::secureRand64());
  std::string tmpFilename = randomId + "_" +
      private_lift::filepath_helpers::getBaseFilename(outputPath_);
  auto tmpFilepath = (tmpDirectory_ / tmpFilename).string();
  XLOG(INFO) << "Writing temporary file to " << tmpFilepath;
  auto tmpFile = std::make_unique<std::ofstream>(tmpFilepath);

  XLOG(INFO) << "Combining " << dataPath_ << " and " << spinePath_ << " into "
             << outputPath_;
  const std::vector<std::string> requiredPublisherCols = {
      "opportunity_timestamp", "test_flag"};
  const std::vector<std::string> requiredPartnerCols = {"event_timestamp"};

  // TODO T86923630: Uncomment this once data validation supports hashed ids
  // Temporary workaround because it breaks on non-int id_ column
  // pid::combiner::validateCsvData(dataInStream);

  // Inspect the headers and verify if this is the publisher or partner dataset
  std::string headerLine = bufferedDataReader->readLine();
  std::vector<std::string> header;
  folly::split(",", headerLine, header);

  bool isPublisherDataset =
      combiner::verifyHeaderContainsCols(header, requiredPublisherCols);
  bool isPartnerDataset =
      combiner::verifyHeaderContainsCols(header, requiredPartnerCols);
  if (isPartnerDataset == isPublisherDataset) {
    XLOG(FATAL) << "Invalid headers for dataset.";
  }

  // TODO: Switch from stringstreams to a real random filename
  std::stringstream idSwapOutFile;
  std::stringstream idMappedOutFile;
  pid::combiner::idSwapMultiKey(
      std::move(bufferedDataReader),
      std::move(bufferedSpineReader),
      idSwapOutFile,
      FLAGS_max_id_column_cnt,
      headerLine,
      spinePath_);
  bufferedDataReader->close();
  bufferedSpineReader->close();

  std::string idSwapOutFileHeaderLine;
  getline(idSwapOutFile, idSwapOutFileHeaderLine);
  std::vector<std::string> idSwapOutFileHeader;
  folly::split(",", idSwapOutFileHeaderLine, idSwapOutFileHeader);
  idSwapOutFile.clear();
  idSwapOutFile.seekg(0);

  std::string line;

  // if partner data, we want to aggregate over remaining columns,
  // add padding, and rename the aggregated columns

  // if its publisher, we want to add the opportunity column based on
  // opportunity_timestamp
  if (isPartnerDataset) {
    // get all columns that are not id_, these are the columns we want to
    // aggregate
    std::vector<std::string> aggregatedCols = idSwapOutFileHeader;
    aggregatedCols.erase(
        std::find(aggregatedCols.begin(), aggregatedCols.end(), "id_"));

    std::stringstream groupByOutFile;
    std::stringstream groupByUnsortedOutFile;
    if (FLAGS_sort_strategy == "sort") {
      pid::combiner::groupBy(
          idSwapOutFile, "id_", aggregatedCols, groupByUnsortedOutFile);
      pid::combiner::sortIds(groupByUnsortedOutFile, groupByOutFile);
    } else if (FLAGS_sort_strategy == "keep_original") {
      pid::combiner::groupBy(
          idSwapOutFile, "id_", aggregatedCols, groupByOutFile);
    } else {
      XLOG(FATAL) << "Invalid sort strategy '" << FLAGS_sort_strategy
                  << "'. Expected 'sort' or 'keep_original'.";
    }

    // add "s" to all aggregated column headers
    std::stringstream renamedColsFile;
    std::vector<std::string> renamedColsVec = idSwapOutFileHeader;
    for (auto& colName : aggregatedCols) {
      auto it = find(renamedColsVec.begin(), renamedColsVec.end(), colName);
      colName.append("s");
      *it = colName;
    }
    renamedColsFile << combiner::vectorToString(renamedColsVec) << "\n";
    getline(groupByOutFile, line);
    renamedColsFile << groupByOutFile.rdbuf();

    // define padding size and add padding to aggregated columns
    std::vector<int32_t> colPaddingSize(
        aggregatedCols.size(), FLAGS_multi_conversion_limit);
    std::stringstream paddingOutFile;

    pid::combiner::addPaddingToCols(
        renamedColsFile, aggregatedCols, colPaddingSize, true, paddingOutFile);

    // ensure conversions are sorted by timestamp
    std::stringstream sortingOutFile;
    std::string sortBy = "event_timestamps";
    std::vector<std::string> listColumns = {"event_timestamps"};
    // It's possible that this is a "valueless" run
    // Also remember that we need to search for the *original* header name
    // since we have pluralized it in a previous step
    if (std::find(
            idSwapOutFileHeader.begin(), idSwapOutFileHeader.end(), "value") !=
        idSwapOutFileHeader.end()) {
      listColumns.push_back("values");
    }
    pid::combiner::sortIntegralValues(
        paddingOutFile, sortingOutFile, sortBy, listColumns);

    *tmpFile << sortingOutFile.rdbuf();
  } else if (isPublisherDataset) {
    // There is no grouping for publisher side,
    // so we can do ID sorting directly.
    std::stringstream sortedOutFile;
    if (FLAGS_sort_strategy == "sort") {
      pid::combiner::sortIds(idSwapOutFile, sortedOutFile);
    } else if (FLAGS_sort_strategy == "keep_original") {
      sortedOutFile << idSwapOutFile.rdbuf();
    } else {
      XLOG(FATAL) << "Invalid sort strategy '" << FLAGS_sort_strategy
                  << "'. Expected 'sort' or 'keep_original'.";
    }

    // We need to get the timestamp index *before* we add the new column
    // Otherwise, we'll get a std::out_of_range exception
    auto timestampIndex =
        combiner::headerIndex(idSwapOutFileHeader, "opportunity_timestamp");
    // add opportunity to header
    idSwapOutFileHeader.insert(idSwapOutFileHeader.end() - 1, "opportunity");
    *tmpFile << combiner::vectorToString(idSwapOutFileHeader) << "\n";

    // add opportunity value.
    // if timestamp is 0, opportunity is 0
    // if timestamp is not 0, opportunity is 1
    getline(sortedOutFile, line); // skip header
    while (getline(sortedOutFile, line)) {
      std::vector<std::string> row;
      folly::split(",", line, row);
      if (row.at(timestampIndex) == "0") {
        row.insert(row.end() - 1, "0");
      } else {
        row.insert(row.end() - 1, "1");
      }
      *tmpFile << combiner::vectorToString(row) << "\n";
    }
  }

  XLOG(INFO) << "Now copying combined data to final output path";
  // Reset underlying unique_ptr to ensure buffer gets flushed
  tmpFile.reset();
  if (outputPath_ != tmpFilepath) {
    // The only time this wouldn't be the case is if tmpFilepath is somehow
    // the final output location (which is possible if the final output is in
    // the same location as our tmpDirectory)
    // TODO: This should never happen if we actually use a tmp filename
    XLOG(INFO) << "Writing " << tmpFilepath << " -> " << outputPath_;

    auto outputType = fbpcf::io::getFileType(outputPath_);
    if (outputType == fbpcf::io::FileType::S3) {
      private_lift::s3_utils::uploadToS3(tmpFilepath, outputPath_);
    } else if (outputType == fbpcf::io::FileType::Local) {
      if (outputPath_.has_parent_path()) {
        std::filesystem::create_directories(outputPath_.parent_path());
      }
      std::filesystem::copy(
          tmpFilepath,
          outputPath_,
          std::filesystem::copy_options::overwrite_existing);
    } else {
      throw std::runtime_error{"Unsupported output destination"};
    }
    // We need to make sure we clean up the tmpfiles now
    std::remove(tmpFilepath.c_str());
  }
  XLOG(INFO) << "Finished combiner.";
}
} // namespace pid
