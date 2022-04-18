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
    std::istream& dataFile,
    std::istream& spineIdFile,
    std::ostream& outFile) {
  XLOG(INFO) << "Started.";
  const int32_t kPaddingSize = FLAGS_padding_size;
  std::vector<std::string> publisherCols = {"ad_id", "timestamp", "is_click"};
  std::vector<std::string> partnerCols = {
      "conversion_timestamp", "conversion_value"};
  std::vector<std::string> shareOptionalCols = {
      "target_id", "action_type", "campaign_metadata", "conversion_metadata"};

  // Inspect the headers and verify if this is the publisher or partner dataset
  std::string headerLine;
  getline(dataFile, headerLine);
  boost::algorithm::trim_if(headerLine, boost::is_any_of("\r"));
  std::vector<std::string> header;
  folly::split(",", headerLine, header);
  dataFile.clear();
  dataFile.seekg(0);

  bool isPublisherDataset = verifyHeaderContainsCols(header, publisherCols);
  bool isPartnerDataset = verifyHeaderContainsCols(header, partnerCols);
  if (isPartnerDataset == isPublisherDataset) {
    XLOG(FATAL) << "Invalid headers for dataset. Header: <"
                << vectorToString(header) << ">. Both headers have status of: <"
                << isPublisherDataset << ">";
  }

  auto& aggregatedCols = isPublisherDataset ? publisherCols : partnerCols;
  // Adding optional columns to aggregatedCols if available
  for (auto& colName : shareOptionalCols) {
    auto iter = std::find(header.begin(), header.end(), colName);
    if (iter != header.end()) {
      aggregatedCols.emplace_back(colName);
    }
  }
  std::vector<int32_t> colPaddingSize(aggregatedCols.size(), kPaddingSize);

  std::stringstream idSwapOutFile;
  idSwapMultiKey(dataFile, spineIdFile, idSwapOutFile, FLAGS_max_id_column_cnt);

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
