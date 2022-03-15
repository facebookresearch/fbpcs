/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

use lazy_static::lazy_static;
use regex::Regex;

pub trait Tokenizer {
    fn tokenize<'a>(&self, line: &'a str) -> Vec<&'a str>;
}

pub struct CSVTokenizer;

impl Tokenizer for CSVTokenizer {
    fn tokenize<'a>(&self, line: &'a str) -> Vec<&'a str> {
        lazy_static! {
            // There are two different matching scenarios:
            // 1) a left bracket, 0 or more of anything that isn't a right bracket, and a right bracket
            // 2) 1 or more of anything that isn't a comma
            static ref RE: Regex = Regex::new("\\[[^\\]]*\\]|[^,]+").unwrap();
        }
        RE.find_iter(line).map(|m| m.as_str()).collect()
    }
}

#[cfg(test)]
mod tests {
    use crate::tokenizer::{CSVTokenizer, Tokenizer};

    #[test]
    fn csv_parse_header() {
        let tokenizer = CSVTokenizer;
        let line = "id_,values,event_timestamps";
        let expected_tokens = vec!["id_", "values", "event_timestamps"];
        let actual_tokens = tokenizer.tokenize(line);
        assert_eq!(expected_tokens, actual_tokens);
    }

    #[test]
    fn csv_parse_row() {
        let tokenizer = CSVTokenizer;
        let line = "14,[0,0,0],[0,0,0]";
        let expected_tokens = vec!["14", "[0,0,0]", "[0,0,0]"];
        let actual_tokens = tokenizer.tokenize(line);
        assert_eq!(expected_tokens, actual_tokens);
    }
}
