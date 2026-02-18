//! Range aware Regex query

use std::sync::Arc;

use tantivy::{
    query::{AutomatonWeight, EnableScoring, Query, Weight},
    schema::Field,
    TantivyError,
};
use tantivy_fst::{Automaton, Regex};

use super::JSON_PREFIX_SEPARATOR;

// Tantivy's in box RegexQuery looks at all possible dictionary values for matches
// For JSON fields this means looking at a lot of values for other fields that can never match
// This class is range aware limiting the number of considered terms

#[derive(Debug, Clone)]
pub struct RangeAwareRegexQuery {
    regex: Arc<SkipAutomaton<Regex>>,
    prefix: String,
    field: Field,
}

impl RangeAwareRegexQuery {
    /// Creates a new RegexQuery from a given pattern
    pub fn from_pattern(
        regex_pattern: &str,
        prefix: &str,
        field: Field,
    ) -> Result<Self, TantivyError> {
        let regex = create_regex(regex_pattern)?;

        let regex = SkipAutomaton::new(
            regex,
            if prefix.is_empty() {
                0
            } else {
                prefix.len() + JSON_PREFIX_SEPARATOR.len()
            },
        );

        Ok(RangeAwareRegexQuery {
            regex: regex.into(),
            prefix: if prefix.is_empty() {
                String::new()
            } else {
                format!("{prefix}\0s")
            },
            field,
        })
    }

    fn specialized_weight(&self) -> AutomatonWeight<SkipAutomaton<Regex>> {
        if self.prefix.is_empty() {
            AutomatonWeight::new(self.field, self.regex.clone())
        } else {
            AutomatonWeight::new_for_json_path(
                self.field,
                self.regex.clone(),
                self.prefix.as_bytes(),
            )
        }
    }
}

impl Query for RangeAwareRegexQuery {
    fn weight(&self, _enabled_scoring: EnableScoring<'_>) -> Result<Box<dyn Weight>, TantivyError> {
        Ok(Box::new(self.specialized_weight()))
    }
}

fn create_regex(pattern: &str) -> Result<Regex, TantivyError> {
    Regex::new(pattern)
        .map_err(|err| TantivyError::InvalidArgument(format!("RanageAwareRegexQuery: {err}")))
}

#[derive(Debug)]
pub struct SkipAutomaton<A> {
    inner: A,
    skip_size: usize,
}

impl<A> SkipAutomaton<A> {
    pub fn new(inner: A, skip_size: usize) -> Self {
        Self { inner, skip_size }
    }
}

#[derive(Clone)]
pub struct SkipAutomatonState<A> {
    count: usize,
    inner: A,
}

impl<A> Automaton for SkipAutomaton<A>
where
    A: Automaton,
    A::State: Clone,
{
    type State = SkipAutomatonState<A::State>;

    fn start(&self) -> Self::State {
        Self::State {
            count: 0,
            inner: self.inner.start(),
        }
    }

    fn is_match(&self, state: &Self::State) -> bool {
        if state.count < self.skip_size {
            false
        } else {
            self.inner.is_match(&state.inner)
        }
    }

    fn accept(&self, state: &Self::State, byte: u8) -> Self::State {
        let mut state = state.clone();

        if state.count < self.skip_size {
            state.count += 1
        } else {
            state.inner = self.inner.accept(&state.inner, byte);
        };

        state
    }

    fn can_match(&self, state: &Self::State) -> bool {
        if state.count < self.skip_size {
            true
        } else {
            self.inner.can_match(&state.inner)
        }
    }

    fn will_always_match(&self, state: &Self::State) -> bool {
        if state.count < self.skip_size {
            false
        } else {
            self.inner.will_always_match(&state.inner)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // For back compat reasons we must ensure the regex language used covers all non-optional items here:
    // https://lucene.apache.org/core/9_0_0/core/org/apache/lucene/util/automaton/RegExp.html
    //
    // These tests validate this

    fn regex_matches(pattern: &str, input: &str) -> bool {
        let regex = create_regex(pattern).expect("Regex should compile");

        let mut state = regex.start();

        for b in input.as_bytes() {
            if regex.will_always_match(&state) {
                return true;
            }

            if !regex.can_match(&state) {
                return false;
            }

            state = regex.accept(&state, *b);
        }

        regex.is_match(&state)
    }

    #[test]
    fn test_regex_empty() {
        assert!(regex_matches("", ""))
    }

    #[test]
    fn test_regex_literal() {
        assert!(regex_matches("abcd", "abcd"))
    }

    #[test]
    fn test_regex_incomplete() {
        assert!(!regex_matches("abcd", "ab"))
    }

    #[test]
    fn test_regex_longer_string() {
        assert!(!regex_matches("ab", "abcd"))
    }

    #[test]
    fn test_regex_substring() {
        assert!(!regex_matches("bc", "abcd"))
    }

    #[test]
    fn test_regex_union() {
        assert!(regex_matches("a|b", "a"));
        assert!(regex_matches("a|b", "b"));
        assert!(!regex_matches("a|b", "c"));
    }

    #[test]
    fn test_regex_question_mark() {
        assert!(regex_matches("a?", "a"));
        assert!(regex_matches("a?", ""));
        assert!(!regex_matches("a?", "b"));
        assert!(!regex_matches("a?", "aa"));
    }

    #[test]
    fn test_regex_asterisk() {
        assert!(regex_matches("a*", "a"));
        assert!(regex_matches("a*", ""));
        assert!(!regex_matches("a*", "b"));
        assert!(regex_matches("a*", "aa"));
    }

    #[test]
    fn test_regex_plus() {
        assert!(regex_matches("a+", "a"));
        assert!(!regex_matches("a+", ""));
        assert!(!regex_matches("a+", "b"));
        assert!(regex_matches("a+", "aa"));
    }

    #[test]
    fn test_regex_n() {
        assert!(regex_matches("a{1}", "a"));
        assert!(!regex_matches("a{1}", ""));
        assert!(!regex_matches("a{1}", "b"));
        assert!(!regex_matches("a{1}", "aa"));
    }

    #[test]
    fn test_regex_n_or_more() {
        assert!(regex_matches("a{1,}", "a"));
        assert!(!regex_matches("a{1,}", ""));
        assert!(!regex_matches("a{1,}", "b"));
        assert!(regex_matches("a{1,}", "aa"));
    }

    #[test]
    fn test_regex_n_m() {
        assert!(regex_matches("a{1,2}", "a"));
        assert!(!regex_matches("a{1,2}", ""));
        assert!(!regex_matches("a{1,2}", "b"));
        assert!(regex_matches("a{1,2}", "aa"));
        assert!(!regex_matches("a{1,2}", "aaa"));
    }

    #[test]
    fn test_regex_char_class() {
        assert!(regex_matches("[ab]", "a"));
        assert!(regex_matches("[ab]", "b"));
        assert!(!regex_matches("[ab]", "c"));
        assert!(!regex_matches("[ab]", "aa"));
    }

    #[test]
    fn test_regex_not_char_class() {
        assert!(!regex_matches("[^ab]", "a"));
        assert!(!regex_matches("[^ab]", "b"));
        assert!(regex_matches("[^ab]", "c"));
        assert!(!regex_matches("[^ab]", "aa"));
    }

    #[test]
    fn test_regex_char_class_range() {
        assert!(regex_matches("[a-z]", "a"));
        assert!(regex_matches("[a-z]", "b"));
        assert!(!regex_matches("[a-z]", "0"));
        assert!(!regex_matches("[a-z]", "aa"));
    }

    #[test]
    fn test_regex_dot() {
        assert!(regex_matches(".", "a"));
        assert!(regex_matches(".", "b"));
        assert!(!regex_matches(".", "aa"));
    }

    #[test]
    fn test_regex_group() {
        assert!(regex_matches("(a)", "a"));
        assert!(!regex_matches("(a)", "b"));
        assert!(!regex_matches("(a)", "aa"));
    }

    #[test]
    fn test_regex_digit() {
        assert!(regex_matches(r"\d", "0"));
        assert!(!regex_matches(r"\d", "b"));
        assert!(!regex_matches(r"\d", "01"));
    }

    #[test]
    fn test_regex_not_digit() {
        assert!(regex_matches(r"\D", "b"));
        assert!(!regex_matches(r"\D", "0"));
        assert!(!regex_matches(r"\D", "ab"));
    }

    #[test]
    fn test_regex_whitespace() {
        assert!(regex_matches(r"\s", " "));
        assert!(!regex_matches(r"\s", "b"));
        assert!(!regex_matches(r"\s", "  "));
    }

    #[test]
    fn test_regex_not_whitespace() {
        assert!(regex_matches(r"\S", "a"));
        assert!(!regex_matches(r"\S", " "));
        assert!(!regex_matches(r"\S", "aa"));
    }

    #[test]
    fn test_regex_word() {
        assert!(regex_matches(r"\w", "a"));
        assert!(!regex_matches(r"\w", "-"));
        assert!(!regex_matches(r"\w", "aa"));
    }

    #[test]
    fn test_regex_not_word() {
        assert!(regex_matches(r"\W", "-"));
        assert!(!regex_matches(r"\W", "a"));
        assert!(!regex_matches(r"\W", "--"));
    }

    #[test]
    fn test_regex_escape() {
        assert!(regex_matches(r"\\", r"\"));
        assert!(!regex_matches(r"\\", "-"));
        assert!(!regex_matches(r"\\", r"\\"));
    }
}
