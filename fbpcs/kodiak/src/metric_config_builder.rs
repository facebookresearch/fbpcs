/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

use crate::column_metadata::ColumnMetadata;
use crate::input_reader::{InputReader, LocalInputReader};
use crate::metric_config::MetricConfig;
use crate::mpc_view::MPCView;
use crate::mpc_view_builder::MPCViewBuilder;

pub struct MetricConfigBuilder<T: ColumnMetadata> {
    input_reader: Box<dyn InputReader>,
    view: MPCView<T>,
}

impl<T: ColumnMetadata> MetricConfigBuilder<T> {
    pub fn new() -> Self {
        Self {
            input_reader: Box::new(LocalInputReader::new("")),
            view: MPCViewBuilder::new().build(),
        }
    }

    pub fn with_input_reader(&mut self, input_reader: Box<dyn InputReader>) -> &Self {
        self.input_reader = input_reader;
        self
    }

    pub fn with_view(&mut self, view: MPCView<T>) -> &Self {
        self.view = view;
        self
    }

    pub fn build(self) -> MetricConfig<T> {
        MetricConfig::new(self.input_reader, self.view)
    }
}
