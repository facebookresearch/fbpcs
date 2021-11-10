/*
 * Copyright (c) Facebook, Inc. and its affiliates.
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
#include "../id_combiner/IdInsert.h"
#include "../id_combiner/IdSwap.h"
#include "../id_combiner/SortIds.h"
#include "../id_combiner/SortIntegralValues.h"
#include "fbpcf/io/FileManagerUtil.h"
#include "fbpcf/io/IInputStream.h"

namespace pid {
void LiftIdSpineFileCombiner::combineFile() {
  auto dataInStreamPtr = fbpcf::io::getInputStream(dataPath_);
  auto spineInStreamPtr = fbpcf::io::getInputStream(spinePath_);
  auto& dataInStream = dataInStreamPtr->get();
  auto& spineInStream = spineInStreamPtr->get();

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
  dataInStream.clear();
  dataInStream.seekg(0);

  // Inspect the headers and verify if this is the publisher or partner dataset
  std::string headerLine;
  getline(dataInStream, headerLine);
  std::vector<std::string> header;
  folly::split(",", headerLine, header);
  dataInStream.clear();
  dataInStream.seekg(0);

  bool isPublisherDataset =
      combiner::verifyHeaderContainsCols(header, requiredPublisherCols);
  bool isPartnerDataset =
      combiner::verifyHeaderContainsCols(header, requiredPartnerCols);
  if (isPartnerDataset == isPublisherDataset) {
    XLOG(FATAL) << "Invalid headers for dataset.";
  }

  // run idSwap followed by idInsert
  // TODO: Switch from stringstreams to a real random filename
  std::stringstream idSwapOutFile;
  std::stringstream idMappedOutFile;
  pid::combiner::idSwap(dataInStream, spineInStream, idMappedOutFile);
  spineInStream.clear();
  spineInStream.seekg(0);
  pid::combiner::idInsert(idMappedOutFile, spineInStream, idSwapOutFile);

  std::string line;

  // if partner data, we want to aggregate over remaining columns,
  // add padding, and rename the aggregated columns

  // if its publisher, we want to add the opportunity column based on
  // opportunity_timestamp
  if (isPartnerDataset) {
    // get all columns that are not id_, these are the columns we want to
    // aggregate
    std::vector<std::string> aggregatedCols = header;
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
    std::vector<std::string> renamedColsVec = header;
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
    if (std::find(header.begin(), header.end(), "value") != header.end()) {
      listColumns.push_back("values");
    }
    pid::combiner::sortIntegralValues(
        paddingOutFile, sortingOutFile, sortBy, listColumns);

    *tmpFile << sortingOutFile.rdbuf();
  } else if (isPublisherDataset) {
    // We need to get the timestamp index *before* we add the new column
    // Otherwise, we'll get a std::out_of_range exception
    auto timestampIndex =
        combiner::headerIndex(header, "opportunity_timestamp");
    // add opportunity to header
    header.insert(header.end() - 1, "opportunity");
    *tmpFile << combiner::vectorToString(header) << "\n";

    // add opportunity value.
    // if timestamp is 0, opportunity is 0
    // if timestamp is not 0, opportunity is 1
    getline(idSwapOutFile, line); // skip header
    while (getline(idSwapOutFile, line)) {
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
