/*
 * Copyright (c) Facebook, Inc. and its affiliates.
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
#include "../id_combiner/IdInsert.h"
#include "../id_combiner/IdSwap.h"
#include "AttributionIdSpineCombinerOptions.h"

namespace pid::combiner {
void attributionIdSpineFileCombiner(
    std::istream& dataFile,
    std::istream& spineIdFile,
    std::ostream& outFile) {
  XLOG(INFO) << "Started.";
  const int32_t kPaddingSize = FLAGS_padding_size;
  const std::vector<std::string> publisherCols = {
      "ad_id", "timestamp", "is_click", "campaign_metadata"};
  const std::vector<std::string> partnerCols = {
      "conversion_timestamp", "conversion_value", "conversion_metadata"};

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
  std::vector<int32_t> colPaddingSize(aggregatedCols.size(), kPaddingSize);

  std::stringstream idMappedOutFile;
  std::stringstream idSwapOutFile;
  idSwap(dataFile, spineIdFile, idMappedOutFile);
  spineIdFile.clear();
  spineIdFile.seekg(0);
  idInsert(idMappedOutFile, spineIdFile, idSwapOutFile);

  std::stringstream groupByOutFile;
  groupBy(idSwapOutFile, "id_", aggregatedCols, groupByOutFile);

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
