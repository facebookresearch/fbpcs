/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

use crate::column_metadata::ColumnMetadata;
use crate::input_reader::InputReader;
use crate::row::Row;
use crate::tokenizer::Tokenizer;

pub struct InputProcessor<C: ColumnMetadata> {
    input_columns: Vec<C>,
    tokenizer: Box<dyn Tokenizer>,
    input_reader: Box<dyn InputReader>,
}

impl<C: ColumnMetadata> InputProcessor<C> {
    fn to_row(&self, tokens: Vec<&str>, header: &[Option<C>]) -> Row<C> {
        let mut row = Row::new();
        for (token, maybe_column) in tokens.into_iter().zip(header) {
            if let Some(column) = maybe_column {
                row.insert(*column, column.from_input(token));
            }
        }
        row
    }


    fn to_header(&self, tokens: Vec<&str>) -> Vec<Option<C>> {
        tokens
            .into_iter()
            .map(|token| {
                C::from_str(token).unwrap_or_else(|_| {
                    panic!("Could not convert token {} to a column name", token)
                })
            })
            // if the column from the file is one of the input columns we are using in the game,
            // then return Some(column). Otherwise, return None
            // Here is an example for why we do this:
            //      * input file has columns: column1,column2,column3
            //      * We only want to process columns: column1,column2
            //      * By returning None for column3, we know not to add that column to our rows
            .map(|column| self.input_columns.contains(&column).then(|| column))
            .collect()
    }
}
