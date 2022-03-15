/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

use crate::column_metadata::ColumnMetadata;
use crate::mpc_metric_dtype::MPCMetricDType;

#[derive(Debug, PartialEq)]
pub struct Row<T: ColumnMetadata> {
    // Dynamic columns because we don't really care about the underlying "data"
    // This allows us to track which columns have already been calculated
    // This struct would likely handle building the DAG to know which columns can
    // be computed next.
    columns: std::collections::HashMap<T, MPCMetricDType>,
}

impl<T: ColumnMetadata> Row<T> {
    pub fn new() -> Self {
        Self {
            columns: std::collections::HashMap::new(),
        }
    }

    pub fn get_data(&self, t: &T) -> Option<&MPCMetricDType> {
        self.columns.get(t)
    }

    pub fn insert(&mut self, k: T, v: MPCMetricDType) -> Option<MPCMetricDType> {
        self.columns.insert(k, v)
    }
}
