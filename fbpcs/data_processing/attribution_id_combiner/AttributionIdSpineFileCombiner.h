/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <filesystem>
#include <unordered_map>
#include <vector>

namespace pid::combiner {
/*
This file implements the AttributionIdSpineFileCombiner that is used to
take the data file and the spine id file (output from pid match step) and
prepare the format for the compute stage.

The function assumes that the publisher columns are:
id_, ad_id, timestamp, is_click

and that the partner columns are:
id_, conversion_timestamp, conversion_value

It then leverages the helpers in id_combiner to group by the id_ column
and aggregate the remaining columns, padding them to all be exactly of length 4.

For example:
If the input data file was:
id_, ad_id, timestamp, is_click
1      a1        t1        1
1      a2        t2        0
2      a1        t1        0

And the input spine id file was:
id_, private_id
1    AAA
2    BBB


Then the output would be:
id_,     ad_id,           timestamp,         is_click
AAA     [0, 0, a1, a2]    [0, 0, t1, t2]     [0, 0, 1, 0]
BBB     [0, 0, 0, a1]     [0, 0, 0, t1]      [0, 0, 0, 0]
*/
void attributionIdSpineFileCombiner(
    std::istream& dataFile,
    std::istream& spineIdFile,
    std::ostream& outFile);
} // namespace pid::combiner
