/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

use crate::input_reader::InputReader;
use crate::mpc_view::MPCView;

pub struct MetricConfig {
    input_reader: InputReader,
    view: MPCView,
}

impl MetricConfig {
    pub fn new(input_reader: InputReader, view: MPCView) -> Self {
        Self { input_reader, view }
    }
}
