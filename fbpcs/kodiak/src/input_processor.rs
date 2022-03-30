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
    pub fn process_input(&self) -> impl Iterator<Item = Row<C>> + '_ {
        let mut raw_lines = self.input_reader.read();
        let header_line = raw_lines
            .next()
            .expect("Input file is empty (has no header)");
        let header_tokens = self.tokenizer.tokenize(&header_line);
        let header = self.to_header(header_tokens);
        // we have to move the ownership of the header into the closure
        // so that it doesn't go out of scope
        raw_lines.map(move |line| {
            let row_tokens = self.tokenizer.tokenize(&line);
            self.to_row(row_tokens, &header)
        })
    }
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

#[cfg(test)]
mod tests {
    use crate::input_processor::InputProcessor;
    use crate::input_reader::InputReader;
    use crate::mpc_metric_dtype::MPCMetricDType;
    use crate::row::Row;
    use crate::shared_test_data::TestEnum;
    use crate::tokenizer::CSVTokenizer;

    //////////////////////// UTIL STRUCT //////////////////////
    struct TestInputReader;

    impl InputReader for TestInputReader {
        fn read(&self) -> Box<dyn Iterator<Item = String>> {
            Box::new(
                vec![
                    "Variant1,Variant2,Variant3".to_string(),
                    "1,2,3".to_string(),
                ]
                .into_iter(),
            )
        }
    }

    //////////////////////// UTIL FUNCTIONS //////////////////////
    fn get_expected_row() -> Row<TestEnum> {
        let mut row = Row::new();
        row.insert(TestEnum::Variant1, MPCMetricDType::MPCInt64(1));
        row.insert(TestEnum::Variant2, MPCMetricDType::MPCInt64(2));
        row
    }

    fn get_expected_header() -> Vec<Option<TestEnum>> {
        vec![Some(TestEnum::Variant1), Some(TestEnum::Variant2), None]
    }

    fn get_expected_columns() -> Vec<TestEnum> {
        vec![TestEnum::Variant1, TestEnum::Variant2]
    }

    fn get_test_processor() -> InputProcessor<TestEnum> {
        InputProcessor {
            input_columns: get_expected_columns(),
            tokenizer: Box::new(CSVTokenizer),
            input_reader: Box::new(TestInputReader),
        }
    }

    //////////////////////// UNIT TESTS //////////////////////
    #[test]
    fn test_to_header() {
        let processor = get_test_processor();
        let tokens = vec!["Variant1", "Variant2", "Variant3"];
        let actual_header = processor.to_header(tokens);
        let expected_header = get_expected_header();
        assert_eq!(expected_header, actual_header);
    }

    #[test]
    fn test_to_row() {
        let processor = get_test_processor();
        let tokens = vec!["1", "2", "3"];
        let actual_row = processor.to_row(tokens, &get_expected_header());
        let expected_row = get_expected_row();
        assert_eq!(expected_row, actual_row);
    }

    #[test]
    fn test_process_input() {
        let processor = get_test_processor();
        let expected_rows = vec![get_expected_row()];
        let actual_rows: Vec<_> = processor.process_input().collect();
        assert_eq!(expected_rows, actual_rows);
    }
}
