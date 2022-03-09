/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

use crate::input_reader::InputReader;
use crate::metric_config::MetricConfig;
use crate::mpc_view::MPCView;
use crate::mpc_view_builder::MPCViewBuilder;

pub struct MetricConfigBuilder {
    input_reader: InputReader,
    view: MPCView,
}

impl MetricConfigBuilder {
    pub fn new() -> Self {
        Self {
            input_reader: InputReader::new(""),
            view: MPCViewBuilder::new().build(),
        }
    }

    pub fn with_input_reader(&mut self, input_reader: InputReader) -> &Self {
        self.input_reader = input_reader;
        self
    }

    pub fn with_view(&mut self, view: MPCView) -> &Self {
        self.view = view;
        self
    }

    pub fn build(self) -> MetricConfig {
        MetricConfig::new(self.input_reader, self.view)
    }
}
