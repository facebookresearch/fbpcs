/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <filesystem>
#include <istream>
#include <ostream>
#include <string>
#include <unordered_map>
#include <vector>

namespace pid::combiner {
/*
This file implements the groupBy that is used to group by
one column's value and aggregate specified column values into a list
Any unspecified columns will be left

For example, if for groupBy index 'id', specified columns are val1 and val 2
and this input file content:
id        val1       val2        val3
1           x          a           v1
1           y          b           v1
2           z          c           v3

The output would be:
id        val1       val2      val3
1        [x, y]     [a, b]       v1
2          [z]        [c]        v3
*/
void groupBy(
    std::istream& inFilePath,
    std::string groupByColumn,
    std::vector<std::string> columnsToAggregate,
    std::ostream& outFilePath);
} // namespace pid::combiner
