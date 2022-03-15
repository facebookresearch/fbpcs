/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

use crate::column_metadata::ColumnMetadata;
use crate::input_reader::InputReader;
use crate::mpc_view::MPCView;

pub struct MetricConfig<T: ColumnMetadata> {
    input_reader: Box<dyn InputReader>,
    view: MPCView<T>,
}

impl<T: ColumnMetadata> MetricConfig<T> {
    pub fn new(input_reader: Box<dyn InputReader>, view: MPCView<T>) -> Self {
        Self { input_reader, view }
    }
}
