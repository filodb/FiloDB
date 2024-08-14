//! Query parsers and builders

use std::ops::Bound;

use nom::{
    number::{complete::le_i64, streaming::le_u16},
    Err, IResult,
};
use num_derive::FromPrimitive;
use tantivy::{
    query::{
        AllQuery, BooleanQuery, EmptyQuery, Occur, Query, RangeQuery, TermQuery, TermSetQuery,
    },
    schema::{Field, IndexRecordOption, Schema},
    TantivyError, Term,
};
use tantivy_utils::query::{
    prefix_query::PrefixQuery, range_aware_regex::RangeAwareRegexQuery, JSON_PREFIX_SEPARATOR,
};

use crate::parser::{parse_string, parse_type_id, AsNomError, ParserError, TypeParseResult};

pub mod cachable_query;

/// Query format
///
/// Queries are complex trees of predicates that must be supported. This prevents us from easily
/// encoding them in primitive types across the JNI boundary.
///
/// To avoid making the Rust code a series of complex reflection operations and to make this
/// as efficient as possible a new binary format is defined for the JVM code to pass to the Rust
/// code inside a byte array.
///
/// Each query entry is encoded starting with a single byte type ID.  See the `QueryTypeId` enum
/// for possible values.  For each child query in a boolean query it is encoded via an 8 bit occur value,
/// a 8 bit type id, and a 16 bit length followed by a UTF-8 string with the specified length.
///
/// As a simple example, consider a boolean query like:
///
/// f1:ABC AND f2:DEF
///
/// The encoded sequence would roughly look like:
///
/// 01 - start of boolean query
///     01 - query must match
///         02 - equals query
///         03 00 - string of length 3
///         41 42 43 - UTF8 encoding of 'ABC'
///     01 - query must match
///         02 - equals query
///         03 00 - string of length 3
///         44 45 46 - UTF8 encoding of 'DEF'
///     00 - end of boolean query

/// Query type encoding
#[derive(FromPrimitive)]
#[repr(u8)]
pub enum QueryTypeId {
    /// A boolean query composed of other queries
    Boolean = 1,
    /// A term match / not match query
    Equals = 2,
    /// A regular express term match / not match query
    Regex = 3,
    /// A term must be in / not in a specified list
    TermIn = 4,
    /// A term must start with / not start with input
    Prefix = 5,
    /// Match all documents
    MatchAll = 6,
    /// Start->End range query on a long field
    LongRange = 7,
}

/// Occurs encoding
#[derive(FromPrimitive)]
#[repr(u8)]
pub enum Occurs {
    /// Query must match
    Must = 1,
    /// Query must not match
    MustNot = 2,
}

pub fn parse_query<'a>(
    input: &'a [u8],
    schema: &Schema,
    default_field: Option<Field>,
) -> IResult<&'a [u8], Box<dyn Query>, ParserError> {
    let (input, type_id) = parse_type_id(input)?;

    match type_id {
        TypeParseResult::Failure(type_id) => Err(Err::Failure(ParserError::UnknownType(type_id))),
        TypeParseResult::Success(QueryTypeId::Boolean) => {
            parse_boolean_query(input, schema, default_field)
        }
        TypeParseResult::Success(QueryTypeId::Equals) => {
            parse_equals_query(input, schema, default_field)
        }
        TypeParseResult::Success(QueryTypeId::Regex) => {
            parse_regex_query(input, schema, default_field)
        }
        TypeParseResult::Success(QueryTypeId::TermIn) => {
            parse_term_in_query(input, schema, default_field)
        }
        TypeParseResult::Success(QueryTypeId::Prefix) => {
            parse_prefix_query(input, schema, default_field)
        }
        TypeParseResult::Success(QueryTypeId::MatchAll) => {
            let query = AllQuery;
            Ok((input, Box::new(query)))
        }
        TypeParseResult::Success(QueryTypeId::LongRange) => {
            parse_long_range_query(input, schema, default_field)
        }
    }
}

fn parse_boolean_query<'a>(
    input: &'a [u8],
    schema: &Schema,
    default_field: Option<Field>,
) -> IResult<&'a [u8], Box<dyn Query>, ParserError> {
    let mut subqueries = vec![];
    let mut next_input = input;
    loop {
        let (input, occur) = parse_type_id(next_input)?;

        let occur = match occur {
            TypeParseResult::Success(Occurs::Must) => tantivy::query::Occur::Must,
            TypeParseResult::Success(Occurs::MustNot) => tantivy::query::Occur::MustNot,
            TypeParseResult::Failure(0) => {
                // End of boolean marker
                next_input = input;
                break;
            }
            TypeParseResult::Failure(occur) => {
                return Err(Err::Failure(ParserError::UnknownOccur(occur)))
            }
        };

        let (input, query) = parse_query(input, schema, default_field)?;

        next_input = input;

        subqueries.push((occur, query));
    }

    // Query optimization - 0 elements or 1 element can be special cased
    let query = if subqueries.is_empty() {
        Box::new(EmptyQuery)
    } else if subqueries.len() == 1 && subqueries[0].0 == Occur::Must {
        subqueries.remove(0).1
    } else {
        Box::new(BooleanQuery::new(subqueries))
    };

    Ok((next_input, query))
}

fn value_with_prefix(prefix: &str, value: &str) -> String {
    format!(
        "{}{}{}",
        prefix,
        if prefix.is_empty() {
            ""
        } else {
            JSON_PREFIX_SEPARATOR
        },
        value
    )
}

fn query_with_field_and_value<T>(
    schema: &Schema,
    default_field: Option<Field>,
    field: &str,
    func: T,
) -> Result<Box<dyn Query>, Err<ParserError>>
where
    T: FnOnce(Field, &str) -> Result<Box<dyn Query>, TantivyError>,
{
    let Some((field, prefix)) = schema.find_field_with_default(field, default_field) else {
        // If it's an invalid field then map to an empty query, which will emulate Lucene's behavior
        return Ok(Box::new(EmptyQuery));
    };

    func(field, prefix).to_nom_err()
}

fn parse_equals_query<'a>(
    input: &'a [u8],
    schema: &Schema,
    default_field: Option<Field>,
) -> IResult<&'a [u8], Box<dyn Query>, ParserError> {
    let (input, column) = parse_string(input)?;
    let (input, text) = parse_string(input)?;

    let query = query_with_field_and_value(schema, default_field, &column, |field, prefix| {
        Ok(Box::new(TermQuery::new(
            Term::from_field_text(field, &value_with_prefix(prefix, &text)),
            IndexRecordOption::Basic,
        )))
    })?;

    Ok((input, query))
}

fn parse_regex_query<'a>(
    input: &'a [u8],
    schema: &Schema,
    default_field: Option<Field>,
) -> IResult<&'a [u8], Box<dyn Query>, ParserError> {
    let (input, column) = parse_string(input)?;
    let (input, text) = parse_string(input)?;

    let query = query_with_field_and_value(schema, default_field, &column, |field, prefix| {
        let query = RangeAwareRegexQuery::from_pattern(&text, prefix, field)?;

        Ok(Box::new(query))
    })?;

    Ok((input, query))
}

fn parse_term_in_query<'a>(
    input: &'a [u8],
    schema: &Schema,
    default_field: Option<Field>,
) -> IResult<&'a [u8], Box<dyn Query>, ParserError> {
    let (input, column) = parse_string(input)?;
    let (input, term_count) = le_u16(input)?;

    // Repeated for each term
    let mut terms = vec![];
    let mut next_input = input;
    for _ in 0..term_count {
        let (input, text) = parse_string(next_input)?;

        if let Some((field, prefix)) = schema.find_field_with_default(&column, default_field) {
            terms.push(Term::from_field_text(
                field,
                &value_with_prefix(prefix, &text),
            ));
        };

        next_input = input;
    }
    let query = TermSetQuery::new(terms);

    Ok((next_input, Box::new(query)))
}

fn parse_prefix_query<'a>(
    input: &'a [u8],
    schema: &Schema,
    default_field: Option<Field>,
) -> IResult<&'a [u8], Box<dyn Query>, ParserError> {
    let (input, column) = parse_string(input)?;
    let (input, text) = parse_string(input)?;

    let query = query_with_field_and_value(schema, default_field, &column, |field, prefix| {
        let query = PrefixQuery::new(&text, prefix, field);

        Ok(Box::new(query))
    })?;

    Ok((input, query))
}

fn parse_long_range_query<'a>(
    input: &'a [u8],
    schema: &Schema,
    _default_field: Option<Field>,
) -> IResult<&'a [u8], Box<dyn Query>, ParserError> {
    let (input, column) = parse_string(input)?;

    let field = schema.get_field(&column).to_nom_err()?;
    let field_name = schema.get_field_entry(field).name();

    // 8 byte start
    let (input, start) = le_i64(input)?;

    // 8 byte end
    let (input, end) = le_i64(input)?;

    let query = RangeQuery::new_i64_bounds(
        field_name.to_string(),
        Bound::Included(start),
        Bound::Included(end),
    );

    Ok((input, Box::new(query)))
}

#[cfg(test)]
mod tests {
    use bytes::BufMut;
    use tantivy::{collector::DocSetCollector, query::Occur};
    use tantivy_utils::field_constants::PART_ID;

    use tantivy_utils::test_utils::{
        build_test_schema, COL1_NAME, COL2_NAME, JSON_ATTRIBUTE1_NAME, JSON_ATTRIBUTE2_NAME,
        JSON_COL_NAME,
    };

    use super::*;

    #[test]
    fn test_parse_equals() {
        let index = build_test_schema();

        let mut buf = vec![];

        let filter = "ABC";

        buf.put_u16_le(COL1_NAME.len() as u16);
        buf.put_slice(COL1_NAME.as_bytes());
        buf.put_u16_le(filter.len() as u16);
        buf.put_slice(filter.as_bytes());

        let (_, query) = parse_equals_query(&buf, &index.schema, None).expect("Should succeed");

        let unboxed = query.downcast_ref::<TermQuery>().unwrap();

        assert_eq!(
            unboxed.term().field(),
            index.schema.get_field(COL1_NAME).unwrap()
        );
        assert_eq!(unboxed.term().value().as_str().unwrap(), filter);

        let collector = DocSetCollector;
        let results = index
            .searcher
            .search(&query, &collector)
            .expect("Should succeed");

        assert_eq!(results.len(), 1);
    }

    #[test]
    fn test_parse_equals_json_field() {
        let index = build_test_schema();

        let mut buf = vec![];

        let filter = "value";

        buf.put_u16_le(JSON_ATTRIBUTE1_NAME.len() as u16);
        buf.put_slice(JSON_ATTRIBUTE1_NAME.as_bytes());
        buf.put_u16_le(filter.len() as u16);
        buf.put_slice(filter.as_bytes());

        let (_, query) = parse_equals_query(&buf, &index.schema, Some(index.json_field))
            .expect("Should succeed");

        let unboxed = query.downcast_ref::<TermQuery>().unwrap();

        assert_eq!(
            unboxed.term().field(),
            index.schema.get_field(JSON_COL_NAME).unwrap()
        );
        assert_eq!(
            unboxed.term().value().as_str().unwrap(),
            format!("{}\0s{}", JSON_ATTRIBUTE1_NAME, filter)
        );

        let collector = DocSetCollector;
        let results = index
            .searcher
            .search(&query, &collector)
            .expect("Should succeed");

        assert_eq!(results.len(), 1);
    }

    #[test]
    fn test_parse_equals_missing_value() {
        let index = build_test_schema();

        let mut buf = vec![];

        let filter = "ABCD";

        buf.put_u16_le(COL1_NAME.len() as u16);
        buf.put_slice(COL1_NAME.as_bytes());
        buf.put_u16_le(filter.len() as u16);

        let err = parse_equals_query(&buf, &index.schema, None).expect_err("Should fail");

        assert_eq!(format!("{err}"), "Parsing requires 4 bytes/chars");
    }

    #[test]
    fn test_parse_equals_invalid_col_name() {
        let index = build_test_schema();

        let mut buf = vec![];

        let filter = "ABCD";

        buf.put_u16_le(4);
        buf.put_slice("invl".as_bytes());
        buf.put_u16_le(filter.len() as u16);
        buf.put_slice(filter.as_bytes());

        let (_, query) = parse_equals_query(&buf, &index.schema, Some(index.json_field))
            .expect("Should succeed");

        let collector = DocSetCollector;
        let results = index
            .searcher
            .search(&query, &collector)
            .expect("Should succeed");

        assert_eq!(results.len(), 0);
    }

    #[test]
    fn test_parse_equals_missing_col_name() {
        let index = build_test_schema();

        let mut buf = vec![];

        buf.put_u16_le(COL1_NAME.len() as u16);

        let err = parse_equals_query(&buf, &index.schema, None).expect_err("Should fail");

        assert_eq!(format!("{err}"), "Parsing requires 4 bytes/chars");
    }

    #[test]
    fn test_parse_boolean() {
        let index = build_test_schema();

        let mut buf = vec![];

        let filter1 = "ABC";
        let filter2 = "abc";

        buf.put_u8(Occurs::Must as u8);
        buf.put_u8(QueryTypeId::Equals as u8);
        buf.put_u16_le(COL1_NAME.len() as u16);
        buf.put_slice(COL1_NAME.as_bytes());
        buf.put_u16_le(filter1.len() as u16);
        buf.put_slice(filter1.as_bytes());

        buf.put_u8(Occurs::MustNot as u8);
        buf.put_u8(QueryTypeId::Equals as u8);
        buf.put_u16_le(COL2_NAME.len() as u16);
        buf.put_slice(COL2_NAME.as_bytes());
        buf.put_u16_le(filter2.len() as u16);
        buf.put_slice(filter2.as_bytes());

        buf.put_u8(0); // End of boolean marker

        let (_, query) = parse_boolean_query(&buf, &index.schema, None).expect("Should succeed");

        let unboxed = query.downcast_ref::<BooleanQuery>().unwrap();

        assert_eq!(unboxed.clauses().len(), 2);

        let (occur, clause) = unboxed.clauses().first().unwrap();
        let clause = clause.downcast_ref::<TermQuery>().unwrap();

        assert_eq!(Occur::Must, *occur);

        assert_eq!(
            clause.term().field(),
            index.schema.get_field(COL1_NAME).unwrap()
        );
        assert_eq!(clause.term().value().as_str().unwrap(), filter1);

        let (occur, clause) = unboxed.clauses().get(1).unwrap();
        let clause = clause.downcast_ref::<TermQuery>().unwrap();

        assert_eq!(Occur::MustNot, *occur);

        assert_eq!(
            clause.term().field(),
            index.schema.get_field(COL2_NAME).unwrap()
        );
        assert_eq!(clause.term().value().as_str().unwrap(), filter2);

        let collector = DocSetCollector;
        let results = index
            .searcher
            .search(&query, &collector)
            .expect("Should succeed");

        assert_eq!(results.len(), 1);
    }

    #[test]
    fn test_parse_boolean_nested() {
        let index = build_test_schema();

        let mut buf = vec![];

        let filter1 = "ABC";
        let filter2 = "abc";

        // Nest boolean queries - (All) And (Equals And Not Equals)
        buf.put_u8(Occurs::Must as u8);
        buf.put_u8(QueryTypeId::Boolean as u8);

        buf.put_u8(Occurs::Must as u8);
        buf.put_u8(QueryTypeId::MatchAll as u8);
        buf.put_u8(0); // End of boolean marker

        buf.put_u8(Occurs::Must as u8);
        buf.put_u8(QueryTypeId::Boolean as u8);

        buf.put_u8(Occurs::Must as u8);
        buf.put_u8(QueryTypeId::Equals as u8);
        buf.put_u16_le(COL1_NAME.len() as u16);
        buf.put_slice(COL1_NAME.as_bytes());
        buf.put_u16_le(filter1.len() as u16);
        buf.put_slice(filter1.as_bytes());

        buf.put_u8(Occurs::MustNot as u8);
        buf.put_u8(QueryTypeId::Equals as u8);
        buf.put_u16_le(COL2_NAME.len() as u16);
        buf.put_slice(COL2_NAME.as_bytes());
        buf.put_u16_le(filter2.len() as u16);
        buf.put_slice(filter2.as_bytes());

        buf.put_u8(0); // End of boolean marker
        buf.put_u8(0); // End of boolean marker

        let (_, query) = parse_boolean_query(&buf, &index.schema, None).expect("Should succeed");

        let collector = DocSetCollector;
        let results = index
            .searcher
            .search(&query, &collector)
            .expect("Should succeed");

        assert_eq!(results.len(), 1);
    }

    #[test]
    fn test_parse_boolean_json_fields() {
        let index = build_test_schema();

        let mut buf = vec![];

        let filter1 = "value";
        let filter2 = "value";

        buf.put_u8(Occurs::Must as u8);
        buf.put_u8(QueryTypeId::Equals as u8);
        buf.put_u16_le(JSON_ATTRIBUTE1_NAME.len() as u16);
        buf.put_slice(JSON_ATTRIBUTE1_NAME.as_bytes());
        buf.put_u16_le(filter1.len() as u16);
        buf.put_slice(filter1.as_bytes());

        buf.put_u8(Occurs::MustNot as u8);
        buf.put_u8(QueryTypeId::Equals as u8);
        buf.put_u16_le(JSON_ATTRIBUTE2_NAME.len() as u16);
        buf.put_slice(JSON_ATTRIBUTE2_NAME.as_bytes());
        buf.put_u16_le(filter2.len() as u16);
        buf.put_slice(filter2.as_bytes());

        buf.put_u8(0); // End of boolean marker

        let (_, query) = parse_boolean_query(&buf, &index.schema, Some(index.json_field))
            .expect("Should succeed");

        let unboxed = query.downcast_ref::<BooleanQuery>().unwrap();

        assert_eq!(unboxed.clauses().len(), 2);

        let (occur, clause) = unboxed.clauses().first().unwrap();
        let clause = clause.downcast_ref::<TermQuery>().unwrap();

        assert_eq!(Occur::Must, *occur);

        assert_eq!(
            clause.term().field(),
            index.schema.get_field(JSON_COL_NAME).unwrap()
        );
        assert_eq!(
            clause.term().value().as_str().unwrap(),
            format!("{}\0s{}", JSON_ATTRIBUTE1_NAME, filter1)
        );

        let (occur, clause) = unboxed.clauses().get(1).unwrap();
        let clause = clause.downcast_ref::<TermQuery>().unwrap();

        assert_eq!(Occur::MustNot, *occur);

        assert_eq!(
            clause.term().field(),
            index.schema.get_field(JSON_COL_NAME).unwrap()
        );
        assert_eq!(
            clause.term().value().as_str().unwrap(),
            format!("{}\0s{}", JSON_ATTRIBUTE2_NAME, filter2)
        );

        let collector = DocSetCollector;
        let results = index
            .searcher
            .search(&query, &collector)
            .expect("Should succeed");

        assert_eq!(results.len(), 1);
    }

    #[test]
    fn test_parse_boolean_missing_end() {
        let index = build_test_schema();

        let mut buf = vec![];

        let filter1 = "ABCD";

        buf.put_u8(Occurs::Must as u8);
        buf.put_u8(QueryTypeId::Equals as u8);
        buf.put_u16_le(COL1_NAME.len() as u16);
        buf.put_slice(COL1_NAME.as_bytes());
        buf.put_u16_le(filter1.len() as u16);
        buf.put_slice(filter1.as_bytes());

        let err = parse_boolean_query(&buf, &index.schema, None).expect_err("Should fail");

        assert_eq!(format!("{err}"), "Parsing requires 1 bytes/chars");
    }

    #[test]
    fn test_parse_boolean_invalid_type() {
        let index = build_test_schema();

        let mut buf = vec![];

        buf.put_u8(Occurs::Must as u8);
        buf.put_u8(255);

        let err = parse_boolean_query(&buf, &index.schema, None).expect_err("Should fail");

        assert_eq!(format!("{err}"), "Parsing Failure: UnknownType(255)");
    }

    #[test]
    fn test_parse_boolean_invalid_occur() {
        let index = build_test_schema();

        let mut buf = vec![];

        buf.put_u8(255);

        let err = parse_boolean_query(&buf, &index.schema, None).expect_err("Should fail");

        assert_eq!(format!("{err}"), "Parsing Failure: UnknownOccur(255)");
    }

    #[test]
    fn test_parse_regex() {
        let index = build_test_schema();

        let mut buf = vec![];

        let filter = "AB.*";

        buf.put_u16_le(COL1_NAME.len() as u16);
        buf.put_slice(COL1_NAME.as_bytes());
        buf.put_u16_le(filter.len() as u16);
        buf.put_slice(filter.as_bytes());

        let (_, query) = parse_regex_query(&buf, &index.schema, None).expect("Should succeed");

        let _ = query
            .downcast_ref::<RangeAwareRegexQuery>()
            .expect("Should create regex");

        let collector = DocSetCollector;
        let results = index
            .searcher
            .search(&query, &collector)
            .expect("Should succeed");

        assert_eq!(results.len(), 1);
    }

    #[test]
    fn test_parse_partial() {
        let index = build_test_schema();

        let mut buf = vec![];

        let filter = "AB";

        buf.put_u16_le(COL1_NAME.len() as u16);
        buf.put_slice(COL1_NAME.as_bytes());
        buf.put_u16_le(filter.len() as u16);
        buf.put_slice(filter.as_bytes());

        let (_, query) = parse_regex_query(&buf, &index.schema, None).expect("Should succeed");

        let _ = query
            .downcast_ref::<RangeAwareRegexQuery>()
            .expect("Should create regex");

        let collector = DocSetCollector;
        let results = index
            .searcher
            .search(&query, &collector)
            .expect("Should succeed");

        // Should not match since it only covers the start of string
        assert_eq!(results.len(), 0);
    }

    #[test]
    fn test_parse_regex_json_field() {
        let index = build_test_schema();

        let mut buf = vec![];

        let filter = "val.*";

        buf.put_u16_le(JSON_ATTRIBUTE1_NAME.len() as u16);
        buf.put_slice(JSON_ATTRIBUTE1_NAME.as_bytes());
        buf.put_u16_le(filter.len() as u16);
        buf.put_slice(filter.as_bytes());

        let (_, query) =
            parse_regex_query(&buf, &index.schema, Some(index.json_field)).expect("Should succeed");

        let _ = query
            .downcast_ref::<RangeAwareRegexQuery>()
            .expect("Should create regex");

        let collector = DocSetCollector;
        let results = index
            .searcher
            .search(&query, &collector)
            .expect("Should succeed");

        assert_eq!(results.len(), 1);
    }

    #[test]
    fn test_parse_regex_missing_value() {
        let index = build_test_schema();

        let mut buf = vec![];

        let filter = "a.*";

        buf.put_u16_le(COL1_NAME.len() as u16);
        buf.put_slice(COL1_NAME.as_bytes());
        buf.put_u16_le(filter.len() as u16);

        let err = parse_regex_query(&buf, &index.schema, None).expect_err("Should fail");

        assert_eq!(format!("{err}"), "Parsing requires 3 bytes/chars");
    }

    #[test]
    fn test_parse_regex_invalid_col_name() {
        let index = build_test_schema();

        let mut buf = vec![];

        let filter = "a.*";

        buf.put_u16_le(4);
        buf.put_slice("invl".as_bytes());
        buf.put_u16_le(filter.len() as u16);
        buf.put_slice(filter.as_bytes());

        let (_, query) =
            parse_regex_query(&buf, &index.schema, Some(index.json_field)).expect("Should succeed");

        let collector = DocSetCollector;
        let results = index
            .searcher
            .search(&query, &collector)
            .expect("Should succeed");

        assert_eq!(results.len(), 0);
    }

    #[test]
    fn test_parse_regex_missing_col_name() {
        let index = build_test_schema();

        let mut buf = vec![];

        buf.put_u16_le(COL1_NAME.len() as u16);

        let err = parse_regex_query(&buf, &index.schema, None).expect_err("Should fail");

        assert_eq!(format!("{err}"), "Parsing requires 4 bytes/chars");
    }

    #[test]
    fn test_parse_term_in() {
        let index = build_test_schema();

        let mut buf = vec![];

        let filter1 = "ABC";
        let filter2 = "DEF";

        buf.put_u16_le(COL1_NAME.len() as u16);
        buf.put_slice(COL1_NAME.as_bytes());
        buf.put_u16_le(2);
        buf.put_u16_le(filter1.len() as u16);
        buf.put_slice(filter1.as_bytes());
        buf.put_u16_le(filter2.len() as u16);
        buf.put_slice(filter2.as_bytes());

        let (_, query) = parse_term_in_query(&buf, &index.schema, None).expect("Should succeed");

        let _ = query
            .downcast_ref::<TermSetQuery>()
            .expect("Should create term set");

        let collector = DocSetCollector;
        let results = index
            .searcher
            .search(&query, &collector)
            .expect("Should succeed");

        assert_eq!(results.len(), 2);
    }

    #[test]
    fn test_parse_term_in_json_field() {
        let index = build_test_schema();

        let mut buf = vec![];

        let filter1 = "value";
        let filter2 = "othervalue";

        buf.put_u16_le(JSON_ATTRIBUTE1_NAME.len() as u16);
        buf.put_slice(JSON_ATTRIBUTE1_NAME.as_bytes());
        buf.put_u16_le(2);
        buf.put_u16_le(filter1.len() as u16);
        buf.put_slice(filter1.as_bytes());
        buf.put_u16_le(filter2.len() as u16);
        buf.put_slice(filter2.as_bytes());

        let (_, query) = parse_term_in_query(&buf, &index.schema, Some(index.json_field))
            .expect("Should succeed");

        let _ = query
            .downcast_ref::<TermSetQuery>()
            .expect("Should create term set");

        let collector = DocSetCollector;
        let results = index
            .searcher
            .search(&query, &collector)
            .expect("Should succeed");

        assert_eq!(results.len(), 2);
    }

    #[test]
    fn test_parse_term_in_incomplete_term_list() {
        let index = build_test_schema();

        let mut buf = vec![];

        let filter1 = "abcd";
        let filter2 = "def";

        buf.put_u16_le(COL1_NAME.len() as u16);
        buf.put_slice(COL1_NAME.as_bytes());
        buf.put_u16_le(3);
        buf.put_u16_le(filter1.len() as u16);
        buf.put_slice(filter1.as_bytes());
        buf.put_u16_le(filter2.len() as u16);
        buf.put_slice(filter2.as_bytes());

        let err = parse_term_in_query(&buf, &index.schema, None).expect_err("Should fail");

        assert_eq!(format!("{err}"), "Parsing requires 2 bytes/chars");
    }

    #[test]
    fn test_parse_prefix() {
        let index = build_test_schema();

        let mut buf = vec![];

        let filter1 = "AB";

        buf.put_u16_le(COL1_NAME.len() as u16);
        buf.put_slice(COL1_NAME.as_bytes());
        buf.put_u16_le(filter1.len() as u16);
        buf.put_slice(filter1.as_bytes());

        let (_, query) = parse_prefix_query(&buf, &index.schema, None).expect("Should succeed");

        let _ = query
            .downcast_ref::<PrefixQuery>()
            .expect("Should create prefix");

        let collector = DocSetCollector;
        let results = index
            .searcher
            .search(&query, &collector)
            .expect("Should succeed");

        assert_eq!(results.len(), 1);
    }

    #[test]
    fn test_parse_prefix_json_field() {
        let index = build_test_schema();

        let mut buf = vec![];

        let filter1 = "val";

        buf.put_u16_le(JSON_ATTRIBUTE1_NAME.len() as u16);
        buf.put_slice(JSON_ATTRIBUTE1_NAME.as_bytes());
        buf.put_u16_le(filter1.len() as u16);
        buf.put_slice(filter1.as_bytes());

        let (_, query) = parse_prefix_query(&buf, &index.schema, Some(index.json_field))
            .expect("Should succeed");

        let _ = query
            .downcast_ref::<PrefixQuery>()
            .expect("Should create prefix");

        let collector = DocSetCollector;
        let results = index
            .searcher
            .search(&query, &collector)
            .expect("Should succeed");

        assert_eq!(results.len(), 1);
    }

    #[test]
    fn test_parse_long_range() {
        let index = build_test_schema();

        let mut buf = vec![];

        buf.put_u16_le(PART_ID.len() as u16);
        buf.put_slice(PART_ID.as_bytes());
        buf.put_i64_le(1);
        buf.put_i64_le(2);

        let (_, query) = parse_long_range_query(&buf, &index.schema, None).expect("Should succeed");

        let _ = query
            .downcast_ref::<RangeQuery>()
            .expect("Should create range");

        let collector = DocSetCollector;
        let results = index
            .searcher
            .search(&query, &collector)
            .expect("Should succeed");

        assert_eq!(results.len(), 1);
    }

    #[test]
    fn test_parse_long_range_missing_end() {
        let index = build_test_schema();

        let mut buf = vec![];

        buf.put_u16_le(COL1_NAME.len() as u16);
        buf.put_slice(COL1_NAME.as_bytes());
        buf.put_i64_le(1);

        let err = parse_long_range_query(&buf, &index.schema, None).expect_err("Should fail");

        assert_eq!(format!("{err}"), "Parsing Error: Nom(Eof)");
    }
}
