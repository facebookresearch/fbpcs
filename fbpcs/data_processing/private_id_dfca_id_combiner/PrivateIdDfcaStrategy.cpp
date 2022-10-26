/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "fbpcs/data_processing/private_id_dfca_id_combiner/PrivateIdDfcaStrategy.h"

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

void PrivateIdDfcaStrategy::aggregate(
    std::stringstream& idSwapOutFile,
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

  if (FLAGS_sort_strategy == "sort") {
    sortIds(idSwapOutFile, outFile);
  } else if (FLAGS_sort_strategy == "keep_original") {
    outFile << idSwapOutFile.rdbuf();
  } else {
    XLOG(FATAL) << "Invalid sort strategy '" << FLAGS_sort_strategy
                << "'. Expected 'sort' or 'keep_original'.";
  }

  outFile.close();
  if (outputPath != tmpFilepath) {
    fbpcf::io::FileIOWrappers::transferFileInParts(tmpFilepath, outputPath);
    std::remove(tmpFilepath.c_str());
  }
}

bool PrivateIdDfcaStrategy::getFileType(std::string headerLine) {
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

  return isPublisherDataset;
}

FileMetaData PrivateIdDfcaStrategy::processHeader(
    const std::shared_ptr<fbpcf::io::BufferedReader>& file) {
  FileMetaData meta;
  std::string headerLine = file->readLine();
  boost::algorithm::trim_if(headerLine, boost::is_any_of("\r"));
  auto isPublisherDataset = getFileType(headerLine);

  auto aggregatedCols = isPublisherDataset ? publisherCols : partnerCols;

  std::vector<std::string> header;
  folly::split(",", headerLine, header);

  meta.aggregatedCols = aggregatedCols;
  meta.isPublisherDataset = isPublisherDataset;
  meta.headerLine = headerLine;
  return meta;
}
} // namespace pid::combiner
