/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

use crate::column_metadata::ColumnMetadata;
use crate::mpc_role::MPCRole;
use crate::mpc_view::MPCView;

pub struct MPCViewBuilder<T: ColumnMetadata> {
    input_columns: Vec<T>,
    helper_columns: Vec<T>,
    metrics: Vec<T>,
    grouping_sets: Vec<Vec<T>>,
}

impl<T: ColumnMetadata> MPCViewBuilder<T> {
    pub fn new() -> Self {
        Self {
            input_columns: Vec::new(),
            helper_columns: Vec::new(),
            metrics: Vec::new(),
            grouping_sets: Vec::new(),
        }
    }

    pub fn with_input_column(&mut self, role: MPCRole, col: T) -> &Self {
        self.input_columns.push(col);
        self
    }

    pub fn with_helper_column(&mut self, col: T) -> &Self {
        self.helper_columns.push(col);
        self
    }

    pub fn with_metric(&mut self, metric: T) -> &Self {
        self.metrics.push(metric);
        self
    }

    pub fn with_grouping_set(&mut self, grouping_set: Vec<T>) -> &Self {
        self.grouping_sets.push(grouping_set);
        self
    }

    pub fn build(self) -> MPCView<T> {
        MPCView::new(
            self.input_columns,
            self.helper_columns,
            self.metrics,
            self.grouping_sets,
        )
    }
}
