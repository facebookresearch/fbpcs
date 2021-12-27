/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <string>
#include <vector>

#include <re2/re2.h>

namespace private_measurement::csv {

// Split an input string into component pieces given a delimiter
const std::vector<std::string> split(
    std::string& str,
    const std::string& delim);

// Same as split, but specifically for comma delimiters.
const std::vector<std::string> splitByComma(
    std::string& str,
    bool supportInnerBrackets);

// Reads a csv from the given file, calling the given function for each line
// Returns true on success, false on failure
bool readCsv(
    const std::string& fileName,
    std::function<void(
        const std::vector<std::string>& header,
        const std::vector<std::string>& parts)> readLine,
    std::function<void(const std::vector<std::string>&)> processHeader =
        [](auto) {});

} // namespace private_measurement::csv
