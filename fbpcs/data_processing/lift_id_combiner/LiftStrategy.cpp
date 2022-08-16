/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "fbpcs/data_processing/lift_id_combiner/LiftStrategy.h"

#include <folly/Random.h>
#include <folly/logging/xlog.h>

#include "fbpcs/data_processing/common/FilepathHelpers.h"
#include "fbpcs/data_processing/id_combiner/AddPaddingToCols.h"
#include "fbpcs/data_processing/id_combiner/DataPreparationHelpers.h"
#include "fbpcs/data_processing/id_combiner/DataValidation.h"
#include "fbpcs/data_processing/id_combiner/GroupBy.h"
#include "fbpcs/data_processing/id_combiner/IdSwapMultiKey.h"
#include "fbpcs/data_processing/id_combiner/SortIds.h"
#include "fbpcs/data_processing/id_combiner/SortIntegralValues.h"
#include "fbpcs/data_processing/lift_id_combiner/LiftIdSpineCombinerOptions.h"

namespace pid::combiner {
void LiftStrategy::aggregate(
    std::stringstream& idSwapOutFile,
    bool isPublisherDataset,
    std::string outputPath,
    std::string tmpDirectory,
    std::string sortStrategy) {
  std::filesystem::path tempDir{tmpDirectory};
  // Get a random ID to avoid potential name collisions if multiple
  // runs at the same time point to the same input file
  auto randomId = std::to_string(folly::Random::secureRand64());
  std::string tmpFilename = randomId + "_" +
      private_lift::filepath_helpers::getBaseFilename(outputPath);
  auto tmpFilepath = (tempDir / tmpFilename);
  XLOG(INFO) << "Writing temporary file to " << tmpFilepath;
  std::ofstream outFile{tmpFilepath};

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
  if (isPublisherDataset) {
    // There is no grouping for publisher side,
    // so we can do ID sorting directly.
    std::stringstream sortedOutFile;
    if (sortStrategy == "sort") {
      pid::combiner::sortIds(idSwapOutFile, sortedOutFile);
    } else if (sortStrategy == "keep_original") {
      sortedOutFile << idSwapOutFile.rdbuf();
    } else {
      XLOG(FATAL) << "Invalid sort strategy '" << sortStrategy
                  << "'. Expected 'sort' or 'keep_original'.";
    }

    // We need to get the timestamp index *before* we add the new column
    // Otherwise, we'll get a std::out_of_range exception
    auto timestampIndex =
        combiner::headerIndex(idSwapOutFileHeader, "opportunity_timestamp");
    // add opportunity to header
    idSwapOutFileHeader.insert(idSwapOutFileHeader.end() - 1, "opportunity");
    outFile << combiner::vectorToString(idSwapOutFileHeader) << "\n";

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
      outFile << combiner::vectorToString(row) << "\n";
    }
  } else {
    // get all columns that are not id_, these are the columns we want to
    // aggregate
    std::vector<std::string> aggregatedCols = idSwapOutFileHeader;
    aggregatedCols.erase(
        std::find(aggregatedCols.begin(), aggregatedCols.end(), "id_"));
    // cohort_id is an optional field that may be provided in the partner data.
    // If used, the following compute stage expects that only one cohort id is
    // provided per user. Thus, remove this column from being aggregated on if
    // present. In theory, a user should only belong to one cohort, so grabbing
    // a random cohort per user should be sufficient.
    aggregatedCols.erase(
        std::remove(aggregatedCols.begin(), aggregatedCols.end(), "cohort_id"),
        aggregatedCols.end());

    std::stringstream groupByOutFile;
    std::stringstream groupByUnsortedOutFile;
    if (sortStrategy == "sort") {
      pid::combiner::groupBy(
          idSwapOutFile, "id_", aggregatedCols, groupByUnsortedOutFile);
      pid::combiner::sortIds(groupByUnsortedOutFile, groupByOutFile);
    } else if (sortStrategy == "keep_original") {
      pid::combiner::groupBy(
          idSwapOutFile, "id_", aggregatedCols, groupByOutFile);
    } else {
      XLOG(FATAL) << "Invalid sort strategy '" << sortStrategy
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

    outFile << sortingOutFile.rdbuf();
  }

  XLOG(INFO) << "Now copying combined data to final output path";
  outFile.close();
  if (outputPath != tmpFilepath) {
    // The only time this wouldn't be the case is if tmpFilepath is somehow
    // the final output location (which is possible if the final output is in
    // the same location as our tmpDirectory)
    // TODO: This should never happen if we actually use a tmp filename
    XLOG(INFO) << "Writing " << tmpFilepath << " -> " << outputPath;
    fbpcf::io::FileIOWrappers::transferFileInParts(tmpFilepath, outputPath);
    std::remove(tmpFilepath.c_str());
  }
}

bool LiftStrategy::getFileType(std::string headerLine) {
  // Inspect the headers and verify if this is the publisher or partner
  // dataset
  std::vector<std::string> header;
  folly::split(",", headerLine, header);

  bool isPublisherDataset =
      combiner::verifyHeaderContainsCols(header, requiredPublisherCols);
  bool isPartnerDataset =
      combiner::verifyHeaderContainsCols(header, requiredPartnerCols);
  if (isPartnerDataset == isPublisherDataset) {
    XLOG(FATAL) << "Invalid headers for dataset. Header: <"
                << vectorToString(header) << ">. Both headers have status of: <"
                << isPublisherDataset << ">";
  }
  return isPublisherDataset;
}

FileMetaData LiftStrategy::processHeader(
    const std::shared_ptr<fbpcf::io::BufferedReader>& file) {
  FileMetaData meta;
  // TODO T86923630: Uncomment this once data validation supports hashed ids
  // Temporary workaround because it breaks on non-int id_ column
  // pid::combiner::validateCsvData(dataInStream);

  // Inspect the headers and verify if this is the publisher or partner dataset
  std::string headerLine = file->readLine();
  std::vector<std::string> header;
  folly::split(",", headerLine, header);
  meta.isPublisherDataset = getFileType(headerLine);
  meta.headerLine = headerLine;
  return meta;
}
} // namespace pid::combiner
