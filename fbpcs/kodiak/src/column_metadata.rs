/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

use crate::mpc_metric_dtype::MPCMetricDType;
use crate::row::Row;
use std::str::FromStr;

pub trait ColumnMetadata: std::cmp::Eq + std::hash::Hash + Sized + Clone + Copy + FromStr {
    /// Used to look up a human-readable name for this metric.
    /// Should be known at compile time, so &'static is fine.
    fn name(&self) -> &'static str;

    /// A list of columns which this column depends on. This will be used
    /// to generate a DAG of columns to be built.
    fn dependencies(&self) -> Vec<Self>;

    /// Compute this column's data for the given row
    fn from_row(&self, r: &Row<Self>) -> MPCMetricDType;

    /// Aggregate all values in this column across the iterator of rows
    /// into the final value
    fn aggregate<I: Iterator<Item = Row<Self>>>(&self, rows: I) -> MPCMetricDType;
}

#[macro_export]
macro_rules! column_metadata {

    (@dependencies ($name:ident, $($variant:ident),*)) => (vec![$($name::$variant),*]);

    ($name:ident {
        $($variant:ident -> [$($deps:ident),*]),*,
    }) => {

        #[derive(Copy, Clone, Debug, PartialEq, Eq, std::hash::Hash)]
        pub enum $name {
            $($variant),*
        }

        impl ColumnMetadata for $name {
            fn name(&self) -> &'static str {
                match self {
                    $($name::$variant => stringify!($variant)),*
                }
            }

            fn dependencies(&self) -> Vec<$name> {
                match self {
                    $($name::$variant => column_metadata!(@dependencies ($name, $($deps),*))),*
                }
            }

            fn from_row(&self, r: &Row<Self>) -> MPCMetricDType {
                self.from_row(r)
            }
            fn aggregate<I: Iterator<Item = Row<Self>>>(&self, rows: I) -> MPCMetricDType {
                self.aggregate(rows)
            }
        }

        impl std::str::FromStr for $name {
            type Err = ();

            // TODO: accept a column name alias in column_metadata macro
            // so that input columns can have multiple mappings
            fn from_str(s: &str) -> Result<Self, ()> {
                match s {
                    $(stringify!($variant) => Ok($name::$variant)),*,
                    _ => Err(())
                }
            }
        }
    };
}

#[cfg(test)]
mod tests {
    use crate::column_metadata::ColumnMetadata;
    use crate::mpc_metric_dtype::MPCMetricDType;
    use crate::row::Row;
    use std::str::FromStr;

    column_metadata! {
        TestEnum {
            Variant1 -> [],
            Variant2 -> [Variant1],
            Variant3 -> [Variant1],
            Variant4 -> [Variant2, Variant3],
        }
    }

    impl TestEnum {
        fn from_row(&self, r: &Row<Self>) -> MPCMetricDType {
            panic!("Undefined for test");
        }
        fn aggregate<I: Iterator<Item = Row<Self>>>(&self, rows: I) -> MPCMetricDType {
            panic!("Undefined for test");
        }
    }

    #[test]
    fn name_generation() {
        assert_eq!(TestEnum::Variant1.name(), "Variant1");
        assert_eq!(TestEnum::Variant2.name(), "Variant2");
        assert_eq!(TestEnum::Variant3.name(), "Variant3");
        assert_eq!(TestEnum::Variant4.name(), "Variant4");
    }

    #[test]
    fn dependency_generation() {
        assert_eq!(TestEnum::Variant1.dependencies(), vec![]);
        assert_eq!(TestEnum::Variant2.dependencies(), vec![TestEnum::Variant1]);
        assert_eq!(TestEnum::Variant3.dependencies(), vec![TestEnum::Variant1]);
        assert_eq!(
            TestEnum::Variant4.dependencies(),
            vec![TestEnum::Variant2, TestEnum::Variant3]
        );
    }

    #[test]
    fn from_str() {
        assert_eq!(TestEnum::Variant1, TestEnum::from_str("Variant1").unwrap());
        assert!(TestEnum::from_str("VariantDNE").is_err());
        assert!(TestEnum::from_str("").is_err());
    }
}
