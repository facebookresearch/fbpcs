/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <functional>
#include <string>
#include <vector>

#include "fbpcf/io/api/BufferedReader.h"
#include "fbpcf/io/api/FileReader.h"

#include "Constants.h"
#include "Csv.h"

namespace private_measurement::csv {

const std::vector<std::string> split(
    std::string& str,
    const std::string& delim) {
  // Preprocessing step: Remove spaces if any
  str.erase(std::remove(str.begin(), str.end(), ' '), str.end());
  std::vector<std::string> tokens;
  re2::RE2 rgx{delim};
  re2::StringPiece input{str}; // Wrap a StringPiece around it

  std::string token;
  while (RE2::Consume(&input, rgx, &token)) {
    tokens.push_back(token);
  }
  return tokens;
}

const std::vector<std::string> splitByComma(
    std::string& str,
    bool supportInnerBrackets) {
  if (supportInnerBrackets) {
    // The pattern here indicates that it's looking for a \[, gets all
    // non-brackets [^\]], then the \]. Otherwise |,
    // it will get all the non commas [^,]. The surrounding () makes it
    // a capture group. ,? means there may or may not be a comma
    return split(str, R"((\[[^\]]+\]|[^,]+),?)");
  } else {
    // split internally uses RE2 which relies on
    // consuming patterns. The pattern here indicates
    // it will get all the non commas [^,]. The surrounding () makes it
    // a capture group. ,? means there may or may not be a comma

    return split(str, "([^,]+),?");
  }
}

bool readCsv(
    const std::string& fileName,
    std::function<
        void(const std::vector<std::string>&, const std::vector<std::string>&)>
        readLine,
    std::function<void(const std::vector<std::string>&)> processHeader) {
  auto inlineReader = std::make_unique<fbpcf::io::FileReader>(fileName);
  auto inlineBufferedReader = std::make_unique<fbpcf::io::BufferedReader>(
      std::move(inlineReader), common::kBufferedReaderChunkSize);

  std::string line = inlineBufferedReader->readLine();
  auto header = splitByComma(line, false);
  processHeader(header);

  while (!inlineBufferedReader->eof()) {
    // Split on commas, but if it looks like we're reading an array
    // like `[1, 2, 3]`, take the whole array
    line = inlineBufferedReader->readLine();
    auto parts = splitByComma(line, true);
    readLine(header, parts);
  }
  inlineBufferedReader->close();
  return true;
}

} // namespace private_measurement::csv
