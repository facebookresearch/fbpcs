/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

use crate::mpc_metric::{MPCMetric, MPCMetricDType};

pub struct Row {
    // Dynamic columns because we don't really care about the underlying "data"
    // This allows us to track which columns have already been calculated
    // This struct would likely handle building the DAG to know which columns can
    // be computed next.
    columns: std::collections::HashMap<String, Box<dyn MPCMetric>>,
}

impl Row {
    fn new() -> Self {
        Self {
            columns: std::collections::HashMap::new(),
        }
    }

    fn get_data(&self, column_name: &str) -> Option<Box<dyn MPCMetricDType>> {
        Some(self.columns.get(column_name)?.data())
    }
}
