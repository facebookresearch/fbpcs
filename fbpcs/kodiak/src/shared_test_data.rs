/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

use crate::column_metadata;
use crate::column_metadata::ColumnMetadata;
use crate::mpc_metric_dtype::MPCMetricDType;
use crate::row::Row;

column_metadata! {
    TestEnum {
        Variant1 -> [],
        Variant2 -> [],
        Variant3 -> [Variant1],
        Variant4 -> [Variant1],
        Variant5 -> [Variant3, Variant4],
        Variant6 -> [Variant2, Variant3],
    }
}

impl TestEnum {
    fn from_row(&self, _r: &Row<Self>) -> MPCMetricDType {
        panic!("Undefined for test");
    }
    fn aggregate<I: Iterator<Item = Row<Self>>>(&self, _rows: I) -> MPCMetricDType {
        panic!("Undefined for test");
    }
}
