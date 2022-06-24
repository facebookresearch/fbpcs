/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "AttributionIdSpineFileCombiner.h"

#include <iomanip>
#include <istream>
#include <ostream>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <vector>

#include <boost/algorithm/string.hpp>
#include <folly/String.h>
#include <folly/logging/xlog.h>
#include <re2/re2.h>

#include "../id_combiner/AddPaddingToCols.h"
#include "../id_combiner/DataPreparationHelpers.h"
#include "../id_combiner/DataValidation.h"
#include "../id_combiner/GroupBy.h"
#include "../id_combiner/IdSwapMultiKey.h"
#include "../id_combiner/SortIds.h"

#include "AttributionIdSpineCombinerOptions.h"

namespace pid::combiner {
void attributionIdSpineFileCombiner(
    std::shared_ptr<fbpcf::io::BufferedReader> dataFile,
    std::shared_ptr<fbpcf::io::BufferedReader> spineIdFile,
    std::ostream& outFile,
    std::string spineIdFilePath) {
  XLOG(INFO) << "Started.";
  const int32_t kPaddingSize = FLAGS_padding_size;
  std::vector<std::string> publisherCols = {"ad_id", "timestamp", "is_click"};
  std::vector<std::string> publisherOptionalCols = {
      "campaign_metadata", "target_id", "action_type"};
  std::vector<std::string> partnerCols = {
      "conversion_timestamp", "conversion_value"};
  std::vector<std::string> partnerOptionalCols = {
      "conversion_metadata", "conversion_target_id", "conversion_action_type"};

  // Inspect the headers and verify if this is the publisher or partner dataset
  std::string headerLine = dataFile->readLine();
  boost::algorithm::trim_if(headerLine, boost::is_any_of("\r"));
  std::vector<std::string> header;
  folly::split(",", headerLine, header);

  bool isPublisherDataset = verifyHeaderContainsCols(header, publisherCols);
  bool isPartnerDataset = verifyHeaderContainsCols(header, partnerCols);
  if (isPartnerDataset == isPublisherDataset) {
    XLOG(FATAL) << "Invalid headers for dataset. Header: <"
                << vectorToString(header) << ">. Both headers have status of: <"
                << isPublisherDataset << ">";
  }

  auto& aggregatedCols = isPublisherDataset ? publisherCols : partnerCols;
  auto& aggregatedOptionalCols =
      isPublisherDataset ? publisherOptionalCols : partnerOptionalCols;

  // Adding optional columns to aggregatedCols if available
  for (auto& colName : aggregatedOptionalCols) {
    auto iter = std::find(header.begin(), header.end(), colName);
    if (iter != header.end()) {
      aggregatedCols.emplace_back(colName);
    }
  }

  // target_id and action_type columns should both or neither exist
  if (isPublisherDataset) {
    bool containsTargetId = verifyHeaderContainsCols(header, {"target_id"});
    bool containsActionType = verifyHeaderContainsCols(header, {"action_type"});
    if (containsTargetId ^ containsActionType) {
      XLOG(FATAL)
          << "Invalid headers for publisher dataset. Header: <"
          << vectorToString(header)
          << ">. Should have both target_id and action_type or neither of them.";
    }
  } else if (isPartnerDataset) {
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

  std::vector<int32_t> colPaddingSize(aggregatedCols.size(), kPaddingSize);

  std::stringstream idSwapOutFile;
  idSwapMultiKey(
      std::move(dataFile),
      std::move(spineIdFile),
      idSwapOutFile,
      FLAGS_max_id_column_cnt,
      headerLine,
      spineIdFilePath);

  std::stringstream groupByOutFile;
  std::stringstream groupByUnsortedOutFile;

  if (FLAGS_sort_strategy == "sort") {
    groupBy(idSwapOutFile, "id_", aggregatedCols, groupByUnsortedOutFile);
    sortIds(groupByUnsortedOutFile, groupByOutFile);
  } else if (FLAGS_sort_strategy == "keep_original") {
    groupBy(idSwapOutFile, "id_", aggregatedCols, groupByOutFile);
  } else {
    XLOG(FATAL) << "Invalid sort strategy '" << FLAGS_sort_strategy
                << "'. Expected 'sort' or 'keep_original'.";
  }

  std::stringstream paddedOutFile;
  addPaddingToCols(
      groupByOutFile, aggregatedCols, colPaddingSize, true, paddedOutFile);

  std::vector<std::string> partnerColsToConvert = {
      "conversion_timestamp", "conversion_value"};
  std::vector<std::string> publisherColsToConvert = {"ad_id", "timestamp"};
  std::vector<std::string> columnsToConvert =
      isPublisherDataset ? publisherColsToConvert : partnerColsToConvert;
  headerColumnsToPlural(paddedOutFile, columnsToConvert, outFile);

  XLOG(INFO) << "Finished.";
}
} // namespace pid::combiner
