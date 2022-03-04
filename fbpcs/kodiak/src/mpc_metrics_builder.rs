/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

pub struct MPCMetricsBuilder {
    input_columns: Vec<Box<dyn MPCColumn>>,
    metrics: Vec<dyn MPCColumn>,
    grouping_sets: Vec<Vec<dyn MPCColumn>>,
}

impl MPCMetricsBuilder {
    pub fn new() -> Self {
        Self {
            input_columns: Vec::new(),
            metrics: Vec::new(),
            grouping_sets: Vec::new(),
        }
    }

    pub fn with_input_column(&mut self, role: MPCRole, col: Box<dyn MPCColumn>) -> Self {
        input_columns.push(col);
        self
    }

    pub fn with_metric(&mut self, metric: Box<dyn MPCColumn>) -> Self {
        metrics.push(metric);
        self
    }

    pub fn with_grouping_set(&mut self, grouping_set: Vec<Box<dyn MPCColumn>>) -> Self {
        grouping_sets.push(grouping_set);
        self
    }

    pub fn build(&self) -> MPCMetrics {
        MPCMetrics {
            input_columns,
            metrics,
            grouping_sets,
        }
    }
}
