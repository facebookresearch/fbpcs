/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

use crate::column_metadata::ColumnMetadata;

pub struct MPCView<T: ColumnMetadata> {
    input_columns: Vec<T>,
    helper_columns: Vec<T>,
    metrics: Vec<T>,
    grouping_sets: Vec<Vec<T>>,
}

impl<T: ColumnMetadata> MPCView<T> {
    pub fn new(
        input_columns: Vec<T>,
        helper_columns: Vec<T>,
        metrics: Vec<T>,
        grouping_sets: Vec<Vec<T>>,
    ) -> Self {
        Self {
            input_columns,
            helper_columns,
            metrics,
            grouping_sets,
        }
    }
}
