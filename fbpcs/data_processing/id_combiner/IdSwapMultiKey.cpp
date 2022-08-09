/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "IdSwapMultiKey.h"
#include "DataPreparationHelpers.h"
#include "folly/Optional.h"

#include <algorithm>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <istream>
#include <numeric>
#include <ostream>
#include <sstream>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include <folly/Random.h>
#include <folly/String.h>
#include <folly/logging/xlog.h>
#include <re2/re2.h>

namespace pid::combiner {
void aggregateLiftNonIdColumns(
    std::vector<std::string> header,
    std::vector<std::vector<std::string>>& dRows) {
  // Step1: We register an aggregation method for each column name.
  std::unordered_set<std::string> randomFromColumns({"test_flag"});
  std::unordered_set<std::string> maxFromColumns({"breakdown_id"});
  std::unordered_set<std::string> minFromColumns({"opportunity_timestamp"});
  std::unordered_set<std::string> sumOfColumns(
      {"total_spend", "num_clicks", "num_impressions"});

  // vector with single vector inside that stores aggregated result
  std::vector<std::vector<std::string>> aggregatedRows(
      1, std::vector<std::string>());

  header.erase(header.begin()); // remove id_ column
  std::vector<std::vector<int>> columnValues(header.size(), std::vector<int>());

  // Step2: We first transform dRows into column by column vector (columnValues)
  for (auto dRow : dRows) {
    if (dRow.size() != header.size()) {
      XLOG(FATAL)
          << "Error: number of non-id columns not consistent with header.";
    }
    for (size_t col = 0; col < dRow.size(); ++col) {
      try {
        int32_t val = std::stoi(dRow[col]);
        columnValues[col].push_back(val);
      } catch (std::exception& err) {
        XLOG(FATAL)
            << "Error: Exception caught during casting string to int.\n"
            << "\tFor PL, non-id columns has to be int to aggregate in case of duplicates.";
      }
    }
  }

  // Step3: For each column stored in columnValues, we first check the column
  // name to find out aggregation method. Then, we aggregate the values using
  // aggregation method. Finally, we add aggregated value into the vector inside
  // of aggregatedRows.
  for (size_t col = 0; col < header.size(); ++col) {
    int32_t val;
    if (randomFromColumns.count(header[col]) > 0) {
      uint32_t random = folly::Random::rand32() % columnValues[col].size();
      val = columnValues[col][random];
    } else if (maxFromColumns.count(header[col]) > 0) {
      val =
          *std::max_element(columnValues[col].begin(), columnValues[col].end());
    } else if (minFromColumns.count(header[col]) > 0) {
      val =
          *std::min_element(columnValues[col].begin(), columnValues[col].end());
    } else if (sumOfColumns.count(header[col]) > 0) {
      val = accumulate(columnValues[col].begin(), columnValues[col].end(), 0);
    } else {
      // if the column name is not registered, we will take min as default.
      val =
          *std::min_element(columnValues[col].begin(), columnValues[col].end());
      XLOG(INFO) << "WARNING: Column name not registered to aggregate.\n"
                 << "\tWe are taking a min of " << header[col] << "\n";
    }
    aggregatedRows[0].push_back(std::to_string(val));
  }

  // Step4: Swap dRows with aggregatedRows. As a result, dRows would have only
  // one line.
  dRows.swap(aggregatedRows);
}

void idSwapMultiKey(
    std::shared_ptr<fbpcf::io::BufferedReader> dataFile,
    std::shared_ptr<fbpcf::io::BufferedReader> spineIdFile,
    std::ostream& outFile,
    int32_t maxIdColumnCnt,
    std::string headerLine,
    std::string spineIdPath,
    bool isPublisherLift) {
  const std::string kCommaSplitRegex = ",";
  const std::string kIdColumnPrefix = "id_";
  const std::string kDefaultNullReplacement = "0";

  XLOG(INFO) << "Starting.";

  std::string line;
  /*
   * Need to create a duplicate reader for spine input file path as its being
   * read multiple times in this method. In the short term,
   * this may end up being inefficient because we make multiple
   * requests to AWS. However, it should not add much walltime,
   * and we can revisit this to store the contents during the
   * first read.
   */
  auto spineReader = std::make_unique<fbpcf::io::FileReader>(spineIdPath);
  auto spineIdFileDup = std::make_shared<fbpcf::io::BufferedReader>(
      std::move(spineReader), pid::combiner::kBufferedReaderChunkSize);
  std::vector<std::string> header;
  folly::split(kCommaSplitRegex, headerLine, header);

  auto idColumnIndices = headerIndices(header, kIdColumnPrefix);

  auto headerSize = header.size();

  // remove all the id columns and add pid column in the beginning
  for (int i = 0; i < idColumnIndices.size(); i++) {
    header.erase(header.begin() + idColumnIndices.at(i) - i);
  }
  header.insert(header.begin(), "id_");
  outFile << vectorToString(header) << "\n";

  // Build a map for <id_ to private_id> from the spineId File
  std::unordered_map<std::string, std::string> idToPrivateIDMap;
  std::string spineRow;
  while (!spineIdFile->eof()) {
    spineRow = spineIdFile->readLine();
    std::vector<std::string> cols;
    folly::split(kCommaSplitRegex, spineRow, cols);
    // expect col 1 in spineIdFile to contain the id_
    auto privId = cols.at(0);

    auto numCols = cols.size();
    if (numCols == 1) {
      continue;
    }

    // PID protocol does not yet allow the same identifiers
    // appearing in the multiple rows. Therefore, we store
    // all the identifiers in the row to idToPrivateIDMap as a key
    // and private id as its value.
    // It starts from i = 1 to skip private id column
    for (size_t i = 1; i < numCols; ++i) {
      auto& id = cols.at(i);
      if (id == "NA" || id == "") {
        // Some private id protocol would return 'NA' as identifier
        // if identifiers mapped to this private id does not exist
        continue;
      }
      idToPrivateIDMap[id] = privId;
    }
  }

  // Build a map for <pid to data> from data file
  std::unordered_map<std::string, std::vector<std::vector<std::string>>>
      pidToDataMap;

  while (!dataFile->eof()) {
    line = dataFile->readLine();
    std::vector<std::string> rowVec;
    folly::split(kCommaSplitRegex, line, rowVec);

    auto rowSize = rowVec.size();
    if (rowSize != headerSize) {
      XLOG(INFO) << "Mismatch between header and row '\n'"
                 << "Header has size " << headerSize << " while row has size "
                 << rowSize << '\n'
                 << "row: " << vectorToString(rowVec) << "\n"
                 << "header: " << vectorToString(header) << "\n";
      std::exit(1);
    }

    // copy vector of row and delete ids
    std::vector<std::string> dataRow = rowVec;
    for (size_t i = 0; i < idColumnIndices.size(); ++i) {
      // On iteration i, we already deleted i columns before
      // so we need to delete a {idColumnIndices.at(i) - i}th column
      dataRow.erase(dataRow.begin() + idColumnIndices.at(i) - i);
    }

    // check if an id has pid allocated.
    // if it does, store non-id row vector as value and pid allocated id as key
    int32_t numIds = 0;
    std::vector<std::string> rowIds;
    auto idSearch = idToPrivateIDMap.begin();
    for (auto idx : idColumnIndices) {
      auto id = rowVec.at(idx);
      if (id == "") {
        continue;
      }
      rowIds.push_back(id);
      idSearch = idToPrivateIDMap.find(id);
      if (idSearch != idToPrivateIDMap.end()) {
        auto pid = idToPrivateIDMap.at(id);
        pidToDataMap[pid].push_back(dataRow);
        break;
      }
      if (++numIds == maxIdColumnCnt) {
        break;
      }
    }

    // If there are no ids, just skip the row
    if (numIds == 0) {
      continue;
    }

    // make sure one of the keys in the row have
    // private_id mapped in the spineFile else throwing
    if (idSearch == idToPrivateIDMap.end()) {
      XLOG(FATAL) << "ID is missing in the spineID file '\n'"
                  << vectorToString(rowIds)
                  << " does not have a corresponding private_id"
                  << "\n";
    }
  }

  // Here we output each row from dataFile to outFile.
  std::string row;
  while (!spineIdFileDup->eof()) {
    row = spineIdFileDup->readLine();
    std::vector<std::string> cols;
    folly::split(kCommaSplitRegex, row, cols);

    // for each row in spine id,
    // look for the corresponding rows in dataFile and
    // output the private_id, along with the data from dataFile
    auto privId = cols.at(0);
    auto numIds = cols.size() - 1;
    auto numNonIds = headerSize - idColumnIndices.size();
    std::vector<std::string> defaultVector(numNonIds, kDefaultNullReplacement);
    auto defaultVectorString = vectorToString(defaultVector);

    if (numIds == 0) {
      // if corresponding row with private id does not exist,
      // we give 0s
      outFile << privId << "," << defaultVectorString << "\n";
      continue;
    }

    // add private id in the left most column (column name = id_)
    // add dataRow which has id columns stripped after private id column
    if (pidToDataMap.count(privId) > 0) {
      auto& dRows = pidToDataMap.at(privId);
      if (isPublisherLift) {
        // For publisher lift dataset, duplicates would result in failure.
        // We are aggregating columns here.
        aggregateLiftNonIdColumns(header, dRows);
      }
      for (auto& dRow : dRows) {
        outFile << privId << "," << vectorToString(dRow) << '\n';
      }
    } else {
      // when associated data does not exist,
      // we give 0s. e.g. identifier is NA
      outFile << privId << "," << defaultVectorString << "\n";
      continue;
    }
  }
  spineIdFileDup->close();
  XLOG(INFO) << "Finished.";
}
} // namespace pid::combiner
