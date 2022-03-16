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
        unimplemented!("Undefined for test");
    }
    fn aggregate<'a, I: Iterator<Item = &'a Row<Self>>>(&self, _rows: I) -> MPCMetricDType
    where
        Self: 'a,
    {
        unimplemented!("Undefined for test");
    }
    fn from_input(&self, input: &str) -> MPCMetricDType {
        let parsed_input = input.parse().unwrap();
        match &self {
            Self::Variant1 => MPCMetricDType::MPCInt64(parsed_input),
            Self::Variant2 => MPCMetricDType::MPCInt64(parsed_input),
            Self::Variant3 => MPCMetricDType::MPCInt64(parsed_input),
            Self::Variant4 => MPCMetricDType::MPCInt64(parsed_input),
            Self::Variant5 => MPCMetricDType::MPCInt64(parsed_input),
            Self::Variant6 => MPCMetricDType::MPCInt64(parsed_input),
        }
    }
}

struct Variant1;
struct Variant2;

impl Variant1 {
    fn from_row(_r: &Row<TestEnumWithAuto>) -> MPCMetricDType {
        MPCMetricDType::MPCInt64(1)
    }

    fn from_input(_input: &str) -> MPCMetricDType {
        MPCMetricDType::MPCInt64(2)
    }

    fn aggregate<'a, I: Iterator<Item = &'a Row<TestEnumWithAuto>>>(_rows: I) -> MPCMetricDType
    where
        TestEnumWithAuto: 'a,
    {
        MPCMetricDType::MPCInt64(3)
    }
}

impl Variant2 {
    fn from_row(_r: &Row<TestEnumWithAuto>) -> MPCMetricDType {
        MPCMetricDType::MPCInt64(4)
    }

    fn from_input(_input: &str) -> MPCMetricDType {
        MPCMetricDType::MPCInt64(5)
    }

    fn aggregate<'a, I: Iterator<Item = &'a Row<TestEnumWithAuto>>>(_rows: I) -> MPCMetricDType
    where
        TestEnumWithAuto: 'a,
    {
        MPCMetricDType::MPCInt64(6)
    }
}

column_metadata! {
    TestEnumWithAuto auto {
        Variant1 -> [],
        Variant2 -> [Variant1],
    }
}
