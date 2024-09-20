//! Query that does a prefix match

use std::sync::Arc;

use tantivy::{
    query::{AutomatonWeight, Query},
    schema::Field,
};
use tantivy_fst::Automaton;

use super::{range_aware_regex::SkipAutomaton, JSON_PREFIX_SEPARATOR};

#[derive(Debug, Clone)]
pub struct PrefixQuery {
    automaton: Arc<SkipAutomaton<PrefixAutomaton>>,
    field: Field,
    json_path: String,
}

impl PrefixQuery {
    pub fn new(prefix: &str, json_path: &str, field: Field) -> Self {
        let automaton = PrefixAutomaton {
            prefix: prefix.as_bytes().into(),
        };
        let automaton = SkipAutomaton::new(
            automaton,
            if json_path.is_empty() {
                0
            } else {
                json_path.len() + JSON_PREFIX_SEPARATOR.len()
            },
        );

        Self {
            automaton: Arc::new(automaton),
            field,
            json_path: json_path.into(),
        }
    }
}

impl Query for PrefixQuery {
    fn weight(
        &self,
        _enable_scoring: tantivy::query::EnableScoring<'_>,
    ) -> tantivy::Result<Box<dyn tantivy::query::Weight>> {
        let automaton = self.automaton.clone();
        let weight: AutomatonWeight<SkipAutomaton<PrefixAutomaton>> = if self.json_path.is_empty() {
            AutomatonWeight::new(self.field, automaton)
        } else {
            AutomatonWeight::new_for_json_path(self.field, automaton, self.json_path.as_bytes())
        };

        Ok(Box::new(weight))
    }
}

#[derive(Debug)]
pub struct PrefixAutomaton {
    prefix: Box<[u8]>,
}

impl Automaton for PrefixAutomaton {
    // The state here is simple - it's the byte offset we're currently checking
    // A value of prefix.len() means we've checked everything and we match
    // A value of MAX means we had a mismatch and will never match
    type State = usize;

    fn start(&self) -> Self::State {
        0
    }

    fn is_match(&self, state: &Self::State) -> bool {
        *state == self.prefix.len()
    }

    fn accept(&self, state: &Self::State, byte: u8) -> Self::State {
        if *state < self.prefix.len() {
            if byte == self.prefix[*state] {
                *state + 1
            } else {
                usize::MAX
            }
        } else {
            *state
        }
    }

    fn can_match(&self, state: &Self::State) -> bool {
        *state != usize::MAX
    }

    fn will_always_match(&self, state: &Self::State) -> bool {
        *state == self.prefix.len()
    }
}
