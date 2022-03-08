/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

pub trait ColumnMetadata {
    fn name(&self) -> &'static str;
    fn dependencies(&self) -> Vec<Self>
    where
        // the Size of Self must be known at compilation time
        Self: Sized;
}

#[macro_export]
macro_rules! column_metadata {
    (@dependencies ($name:ident, $($variant:ident),*)) => (vec![$($name::$variant),*]);

    ($name:ident {
        $($variant:ident -> [$($deps:ident),*]),*,
    }) => {

        #[derive(Debug, PartialEq, Eq)]
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
        }
    };
}

#[cfg(test)]
mod tests {
    use crate::column_metadata::ColumnMetadata;

    column_metadata! {
        TestEnum {
            Variant1 -> [],
            Variant2 -> [Variant1],
            Variant3 -> [Variant1],
            Variant4 -> [Variant2, Variant3],
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
}
