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

    /// convert a data cell to an MPCMetricDType
    fn from_input(&self, input: &str) -> MPCMetricDType;

    /// Aggregate all values in this column across the iterator of rows
    /// into the final value
    fn aggregate<'a, I: Iterator<Item = &'a Row<Self>>>(&self, rows: I) -> MPCMetricDType
    where
        Self: 'a;
}

// TODO: This could be a derive macro if we really want to get fancy
#[macro_export]
macro_rules! column_metadata {
    (@dependencies ($name:ident, $($variant:ident),*)) => (vec![$($name::$variant),*]);

    (@genmethod ($self:ident, $method_name:ident, $args:expr, $name:ident, $($variant:ident),*)) => {
        match $self {
            $($name::$variant => $variant::$method_name($args)),*
        }
    };

    (@enum_decl ($name:ident, $($variant:ident),*)) => {
        #[derive(Copy, Clone, Debug, PartialEq, Eq, std::hash::Hash)]
        pub enum $name {
            $($variant),*
        }
    };

    (@base_methods $name:ident {
        $($variant:ident -> [$($deps:ident),*]),*,
    }) => {
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

        fn from_input(&self, input: &str) -> MPCMetricDType {
            self.from_input(input)
        }

        fn aggregate<'a, I: Iterator<Item = &'a Row<Self>>>(&self, rows: I) -> MPCMetricDType
        where Self: 'a {
            self.aggregate(rows)
        }
    };

    // TODO: deps is unused, this could be simplified
    (@auto_methods $name:ident {
        $($variant:ident -> [$($deps:ident),*]),*,
    }) => {
        fn from_row(&self, r: &Row<Self>) -> MPCMetricDType {
            column_metadata!(@genmethod (self, from_row, r, $name, $($variant),*))
        }

        fn from_input(&self, input: &str) -> MPCMetricDType {
            column_metadata!(@genmethod (self, from_input, input, $name, $($variant),*))
        }

        fn aggregate<'a, I: Iterator<Item = &'a Row<Self>>>(&self, rows: I) -> MPCMetricDType
        where Self: 'a {
            column_metadata!(@genmethod (self, aggregate, rows, $name, $($variant),*))
        }
    };

    // TODO: deps is unused, this could be simplified
    (@fromstr_impl $name:ident {
        $($variant:ident -> [$($deps:ident),*]),*,
    }) => {
        // TODO: This may not be what we want
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

    ($name:ident {
        $($variant:ident -> [$($deps:ident),*]),*,
    }) => {
        column_metadata!(@enum_decl ($name, $($variant),*));

        impl ColumnMetadata for $name {
            column_metadata!(@base_methods $name {
                $($variant -> [$($deps),*]),*,
            });
        }

        column_metadata!(@fromstr_impl $name {
            $($variant -> [$($deps),*]),*,
        });
    };

    ($name:ident auto {
        $($variant:ident -> [$($deps:ident),*]),*,
    }) => {
        column_metadata!(@enum_decl ($name, $($variant),*));

        impl ColumnMetadata for $name {
            column_metadata!(@base_methods $name {
                $($variant -> [$($deps),*]),*,
            });
        }

        impl $name {
            column_metadata!(@auto_methods $name {
                $($variant -> [$($deps),*]),*,
            });
        }

        column_metadata!(@fromstr_impl $name {
            $($variant -> [$($deps),*]),*,
        });
    };

}

#[cfg(test)]
mod tests {
    use crate::column_metadata::ColumnMetadata;
    use crate::mpc_metric_dtype::MPCMetricDType;
    use crate::row::Row;
    use crate::shared_test_data::{TestEnum, TestEnumWithAuto};
    use std::str::FromStr;

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
        assert_eq!(TestEnum::Variant2.dependencies(), vec![]);
        assert_eq!(TestEnum::Variant3.dependencies(), vec![TestEnum::Variant1]);
        assert_eq!(TestEnum::Variant4.dependencies(), vec![TestEnum::Variant1]);
        assert_eq!(
            TestEnum::Variant5.dependencies(),
            vec![TestEnum::Variant3, TestEnum::Variant4]
        );
        assert_eq!(
            TestEnum::Variant6.dependencies(),
            vec![TestEnum::Variant2, TestEnum::Variant3]
        );
    }

    #[test]
    fn from_str() {
        assert_eq!(TestEnum::Variant1, TestEnum::from_str("Variant1").unwrap());
        assert!(TestEnum::from_str("VariantDNE").is_err());
        assert!(TestEnum::from_str("").is_err());
    }

    #[test]
    fn auto_from_row() {
        let r = Row::<TestEnumWithAuto>::new();
        assert_eq!(
            TestEnumWithAuto::Variant1.from_row(&r),
            MPCMetricDType::MPCInt64(1)
        );
        assert_eq!(
            TestEnumWithAuto::Variant2.from_row(&r),
            MPCMetricDType::MPCInt64(4)
        );
    }

    #[test]
    fn auto_from_input() {
        let inp = "";
        assert_eq!(
            TestEnumWithAuto::Variant1.from_input(inp),
            MPCMetricDType::MPCInt64(2)
        );
        assert_eq!(
            TestEnumWithAuto::Variant2.from_input(inp),
            MPCMetricDType::MPCInt64(5)
        );
    }

    #[test]
    fn auto_aggregate() {
        let rows = vec![Row::<TestEnumWithAuto>::new()];
        assert_eq!(
            TestEnumWithAuto::Variant1.aggregate(rows.iter()),
            MPCMetricDType::MPCInt64(3)
        );
        assert_eq!(
            TestEnumWithAuto::Variant2.aggregate(rows.iter()),
            MPCMetricDType::MPCInt64(6)
        );
    }
}
