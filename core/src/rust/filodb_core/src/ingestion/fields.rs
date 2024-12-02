//! Working with field data

use std::collections::BTreeMap;

use nom::{Err, IResult};
use num_derive::FromPrimitive;
use tantivy::schema::Schema;

use crate::{
    parser::{parse_string, parse_type_id, AsNomError, ParserError, TypeParseResult},
    state::IngestingDocument,
};

#[derive(FromPrimitive)]
#[repr(u8)]
enum FieldTypeId {
    Indexed = 1,
    Map = 2,
    Multicolumn = 3,
}

pub fn add_fields<'a>(
    input: &'a [u8],
    doc: &mut IngestingDocument,
    schema: &Schema,
) -> IResult<&'a [u8], (), ParserError> {
    let mut next_input = input;

    while !next_input.is_empty() {
        let (input, type_id) = parse_type_id(next_input)?;

        let (input, _) = match type_id {
            TypeParseResult::Success(FieldTypeId::Indexed) => {
                parse_indexed_field(input, doc, schema)?
            }
            TypeParseResult::Success(FieldTypeId::Map) => parse_map_field(input, doc)?,
            TypeParseResult::Success(FieldTypeId::Multicolumn) => {
                parse_multicolumn_field(input, doc, schema)?
            }
            TypeParseResult::Failure(type_id) => {
                return Err(Err::Failure(ParserError::UnknownType(type_id)))
            }
        };

        next_input = input;
    }

    Ok((next_input, ()))
}

fn parse_indexed_field<'a>(
    input: &'a [u8],
    doc: &mut IngestingDocument,
    schema: &Schema,
) -> IResult<&'a [u8], (), ParserError> {
    let (input, field_name) = parse_string(input)?;
    let (input, value) = parse_string(input)?;

    let field = schema.get_field(&field_name).to_nom_err()?;

    doc.doc.add_text(field, value);
    doc.field_names.push(field_name.to_string());

    Ok((input, ()))
}

fn parse_map_field<'a>(
    input: &'a [u8],
    doc: &mut IngestingDocument,
) -> IResult<&'a [u8], (), ParserError> {
    let (input, map_name) = parse_string(input)?;
    let (input, field_name) = parse_string(input)?;
    let (input, value) = parse_string(input)?;

    // Create new map for this map column if needed
    if !doc.map_values.contains_key(map_name.as_ref()) {
        doc.map_values.insert(map_name.to_string(), BTreeMap::new());
    }

    // Capture value
    doc.map_values
        .get_mut(map_name.as_ref())
        .ok_or(Err::Failure(ParserError::InternalMapError))?
        .insert(field_name.to_string(), value.to_string().into());
    doc.field_names.push(field_name.to_string());

    Ok((input, ()))
}

fn parse_multicolumn_field<'a>(
    input: &'a [u8],
    doc: &mut IngestingDocument,
    schema: &Schema,
) -> IResult<&'a [u8], (), ParserError> {
    let (input, field_name) = parse_string(input)?;
    let (input, value) = parse_string(input)?;

    let field = schema.get_field(&field_name).to_nom_err()?;

    doc.doc.add_text(field, value);
    doc.field_names.push(field_name.to_string());

    Ok((input, ()))
}

#[cfg(test)]
mod tests {
    use bytes::BufMut;
    use tantivy::{schema::OwnedValue, Document};

    use tantivy_utils::test_utils::{
        build_test_schema, COL1_NAME, JSON_ATTRIBUTE1_NAME, JSON_COL_NAME,
    };

    use super::*;

    #[test]
    fn test_parse_indexed_field() {
        let mut doc = IngestingDocument::default();
        let index = build_test_schema();

        let mut buf = vec![];

        let expected = "abcd";

        buf.put_u16_le(COL1_NAME.len() as u16);
        buf.put_slice(COL1_NAME.as_bytes());
        buf.put_u16_le(expected.len() as u16);
        buf.put_slice(expected.as_bytes());

        let _ = parse_indexed_field(&buf, &mut doc, &index.schema).expect("Should succeed");

        assert!(doc.field_names.contains(&COL1_NAME.to_string()));
        assert_eq!(
            **doc
                .doc
                .get_sorted_field_values()
                .first()
                .unwrap()
                .1
                .first()
                .unwrap(),
            OwnedValue::Str(expected.into())
        );
    }

    #[test]
    fn test_parse_map_field() {
        let mut doc = IngestingDocument::default();

        let mut buf = vec![];

        let expected = "abcd";

        buf.put_u16_le(JSON_COL_NAME.len() as u16);
        buf.put_slice(JSON_COL_NAME.as_bytes());
        buf.put_u16_le(JSON_ATTRIBUTE1_NAME.len() as u16);
        buf.put_slice(JSON_ATTRIBUTE1_NAME.as_bytes());
        buf.put_u16_le(expected.len() as u16);
        buf.put_slice(expected.as_bytes());

        let _ = parse_map_field(&buf, &mut doc).expect("Should succeed");

        assert!(doc.field_names.contains(&JSON_ATTRIBUTE1_NAME.to_string()));
        assert_eq!(
            *doc.map_values
                .get(JSON_COL_NAME)
                .unwrap()
                .get(JSON_ATTRIBUTE1_NAME)
                .unwrap(),
            OwnedValue::Str(expected.into())
        );
    }

    #[test]
    fn test_parse_multicolumn_field() {
        let mut doc = IngestingDocument::default();
        let index = build_test_schema();

        let mut buf = vec![];

        let expected = "abcd";

        buf.put_u16_le(COL1_NAME.len() as u16);
        buf.put_slice(COL1_NAME.as_bytes());
        buf.put_u16_le(expected.len() as u16);
        buf.put_slice(expected.as_bytes());

        let _ = parse_multicolumn_field(&buf, &mut doc, &index.schema).expect("Should succeed");

        assert!(doc.field_names.contains(&COL1_NAME.to_string()));
        assert_eq!(
            **doc
                .doc
                .get_sorted_field_values()
                .first()
                .unwrap()
                .1
                .first()
                .unwrap(),
            OwnedValue::Str(expected.into())
        );
    }
}
