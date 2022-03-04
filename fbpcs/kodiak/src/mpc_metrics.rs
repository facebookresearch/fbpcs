/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

pub struct MPCMetrics {
    input_columns: Vec<Box<dyn MPCColumn>>,
    metrics: Vec<dyn MPCColumn>,
    grouping_sets: Vec<Vec<dyn MPCColumn>>,
}

impl MPCMetrics {
    pub fn new(
        input_columns: Vec<Box<dyn MPCColumn>>,
        metrics: Vec<dyn MPCColumn>,
        grouping_sets: Vec<Vec<dyn MPCColumn>>,
    ) -> Self {
        Self {
            input_columns,
            metrics,
            grouping_sets,
        }
    }
}
