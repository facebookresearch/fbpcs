/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <istream>
#include <ostream>
#include <string>
#include <vector>

namespace pid::combiner {
/*
sortIntegralValues is used to sort a stream with list elements according to the
given sortBy vector. For example, if called with sortBy = {"b"} and the dataset
looks like this:
a,b,c
0,[2,1],[3,4]
1,[1,1],[2,5]
2,[1,2],[6,7]

then the output would be:
0,[1,2],[4,3]
1,[1,1],[2,5]
2,[1,2],[6,7]

The listColumns parameter is used to denote which columns are *also* list-based
and should be sorted according to the same permutation as the sortBy column.
*/
void sortIntegralValues(
    std::istream& inStream,
    std::ostream& outStream,
    const std::string& sortBy,
    const std::vector<std::string>& listColumns);
} // namespace pid::combiner
