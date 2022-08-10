/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "fbpcs/data_processing/attribution_id_combiner/AttributionStrategy.h"

#include <folly/Random.h>
#include <folly/logging/xlog.h>

#include <boost/algorithm/string.hpp>
#include "fbpcf/io/api/FileIOWrappers.h"
#include "fbpcs/data_processing/common/FilepathHelpers.h"
#include "fbpcs/data_processing/id_combiner/AddPaddingToCols.h"
#include "fbpcs/data_processing/id_combiner/DataPreparationHelpers.h"
#include "fbpcs/data_processing/id_combiner/DataValidation.h"
#include "fbpcs/data_processing/id_combiner/GroupBy.h"
#include "fbpcs/data_processing/id_combiner/SortIds.h"

namespace pid::combiner {

void AttributionStrategy::aggregate(
    std::stringstream& idSwapOutFile,
    FileMetaData& meta,
    std::string outputPath) {
  std::filesystem::path tmpDirectory{FLAGS_tmp_directory};
  // Get a random ID to avoid potential name collisions if multiple
  // runs at the same time point to the same input file
  auto randomId = std::to_string(folly::Random::secureRand64());
  std::string tmpFilename = randomId + "_" +
      private_lift::filepath_helpers::getBaseFilename(outputPath);
  auto tmpFilepath = tmpDirectory / tmpFilename;
  XLOG(INFO) << "Writing temporary file to " << tmpFilepath;
  std::ofstream outFile{tmpFilepath};

  std::vector<int32_t> colPaddingSize(
      meta.aggregatedCols.size(), FLAGS_padding_size);
  std::stringstream groupByOutFile;
  std::stringstream groupByUnsortedOutFile;
  if (FLAGS_sort_strategy == "sort") {
    groupBy(idSwapOutFile, "id_", meta.aggregatedCols, groupByUnsortedOutFile);
    sortIds(groupByUnsortedOutFile, groupByOutFile);
  } else if (FLAGS_sort_strategy == "keep_original") {
    groupBy(idSwapOutFile, "id_", meta.aggregatedCols, groupByOutFile);
  } else {
    XLOG(FATAL) << "Invalid sort strategy '" << FLAGS_sort_strategy
                << "'. Expected 'sort' or 'keep_original'.";
  }

  std::stringstream paddedOutFile;
  addPaddingToCols(
      groupByOutFile, meta.aggregatedCols, colPaddingSize, true, paddedOutFile);

  std::vector<std::string> partnerColsToConvert = {
      "conversion_timestamp", "conversion_value"};
  std::vector<std::string> publisherColsToConvert = {"ad_id", "timestamp"};
  std::vector<std::string> columnsToConvert =
      meta.isPublisherDataset ? publisherColsToConvert : partnerColsToConvert;
  headerColumnsToPlural(paddedOutFile, columnsToConvert, outFile);

  outFile.close();
  if (outputPath != tmpFilepath) {
    fbpcf::io::FileIOWrappers::transferFileInParts(tmpFilepath, outputPath);
    std::remove(tmpFilepath.c_str());
  }
}

bool AttributionStrategy::getFileType(std::string headerLine) {
  // Inspect the headers and verify if this is the publisher or partner
  // dataset
  std::vector<std::string> header;
  folly::split(",", headerLine, header);

  bool isPublisherDataset = verifyHeaderContainsCols(header, publisherCols);
  bool isPartnerDataset = verifyHeaderContainsCols(header, partnerCols);
  if (isPartnerDataset == isPublisherDataset) {
    XLOG(FATAL) << "Invalid headers for dataset. Header: <"
                << vectorToString(header) << ">. Both headers have status of: <"
                << isPublisherDataset << ">";
  }

  if (isPublisherDataset) {
    // target_id and action_type columns should both or neither exist
    bool containsTargetId = verifyHeaderContainsCols(header, {"target_id"});
    bool containsActionType = verifyHeaderContainsCols(header, {"action_type"});
    if (containsTargetId ^ containsActionType) {
      XLOG(FATAL)
          << "Invalid headers for publisher dataset. Header: <"
          << vectorToString(header)
          << ">. Should have both target_id and action_type or neither of them.";
    }
  } else if (isPartnerDataset) {
    // target_id and action_type columns should both or neither exist
    bool containsTargetId =
        verifyHeaderContainsCols(header, {"conversion_target_id"});
    bool containsActionType =
        verifyHeaderContainsCols(header, {"conversion_action_type"});
    if (containsTargetId ^ containsActionType) {
      XLOG(FATAL)
          << "Invalid headers for partner dataset. Header: <"
          << vectorToString(header)
          << ">. Should have both conversion_target_id and conversion_action_type or neither of them.";
    }
  }
  return isPublisherDataset;
}

FileMetaData AttributionStrategy::processHeader(
    const std::shared_ptr<fbpcf::io::BufferedReader>& file) {
  FileMetaData meta;
  std::string headerLine = file->readLine();
  boost::algorithm::trim_if(headerLine, boost::is_any_of("\r"));
  auto isPublisherDataset = getFileType(headerLine);

  auto aggregatedCols = isPublisherDataset ? publisherCols : partnerCols;
  auto aggregatedOptionalCols =
      isPublisherDataset ? publisherOptionalCols : partnerOptionalCols;

  std::vector<std::string> header;
  folly::split(",", headerLine, header);
  // Adding optional columns to aggregatedCols if available
  for (auto& colName : aggregatedOptionalCols) {
    auto iter = std::find(header.begin(), header.end(), colName);
    if (iter != header.end()) {
      aggregatedCols.emplace_back(colName);
    }
  }

  meta.aggregatedCols = aggregatedCols;
  meta.isPublisherDataset = isPublisherDataset;
  meta.headerLine = headerLine;
  return meta;
}
} // namespace pid::combiner
