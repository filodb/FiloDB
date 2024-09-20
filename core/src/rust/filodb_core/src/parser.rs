//! Binary parser helpers

use std::borrow::Cow;

use nom::{
    bytes::streaming::take,
    error::{ErrorKind, ParseError},
    number::streaming::{le_u16, u8},
    IResult,
};
use num_traits::FromPrimitive;
use tantivy::TantivyError;
use thiserror::Error;

/// Error type for query parsing issues
#[derive(Error, Debug)]
pub enum ParserError {
    #[error("Core parsing error: {0:?}")]
    Nom(ErrorKind),
    #[error("Index error: {0}")]
    IndexError(#[from] TantivyError),
    #[error("Unknown type byte: {0}")]
    UnknownType(u8),
    #[error("Unknown occur byte: {0}")]
    UnknownOccur(u8),
    #[error("Internal map error")]
    InternalMapError,
}

pub trait AsNomError<T> {
    fn to_nom_err(self) -> Result<T, nom::Err<ParserError>>;
}

impl<T> AsNomError<T> for Result<T, TantivyError> {
    fn to_nom_err(self) -> Result<T, nom::Err<ParserError>> {
        match self {
            Err(e) => Err(nom::Err::Failure(e.into())),
            Ok(x) => Ok(x),
        }
    }
}

impl<'a> ParseError<&'a [u8]> for ParserError {
    fn from_error_kind(_input: &'a [u8], kind: ErrorKind) -> Self {
        ParserError::Nom(kind)
    }

    fn append(_input: &'a [u8], _kind: ErrorKind, other: Self) -> Self {
        other
    }
}

pub fn parse_string(input: &[u8]) -> IResult<&[u8], Cow<'_, str>, ParserError> {
    let (input, length) = le_u16(input)?;
    let (input, string_data) = take(length)(input)?;

    Ok((input, String::from_utf8_lossy(string_data)))
}

#[derive(PartialEq, Debug)]
pub enum TypeParseResult<T> {
    Success(T),
    Failure(u8),
}

impl<T> From<u8> for TypeParseResult<T>
where
    T: FromPrimitive,
{
    fn from(value: u8) -> Self {
        match T::from_u8(value) {
            Some(val) => Self::Success(val),
            None => Self::Failure(value),
        }
    }
}

pub fn parse_type_id<T>(input: &[u8]) -> IResult<&[u8], TypeParseResult<T>, ParserError>
where
    T: FromPrimitive,
{
    let (input, type_id) = u8(input)?;
    Ok((input, type_id.into()))
}

#[cfg(test)]
mod tests {
    use bytes::BufMut;
    use num_derive::FromPrimitive;

    use super::*;

    #[test]
    fn test_parse_string() {
        let mut buf = vec![];

        let expected = "abcd";

        buf.put_u16_le(expected.len() as u16);
        buf.put_slice(expected.as_bytes());

        let (_, result) = parse_string(&buf).expect("Should succeed");

        assert_eq!(result, expected);
    }

    #[test]
    fn test_parse_empty_string() {
        let mut buf = vec![];

        buf.put_u16_le(0);

        let (_, result) = parse_string(&buf).expect("Should succeed");

        assert_eq!(result, "");
    }

    #[derive(FromPrimitive, Debug, PartialEq)]
    #[repr(u8)]
    pub enum TestTypeId {
        Val1 = 1,
        Val2 = 2,
    }

    #[test]
    fn test_parse_type_id() {
        let mut buf = vec![];

        buf.put_u8(1);

        let (_, result) = parse_type_id(&buf).expect("Should succeed");

        assert_eq!(result, TypeParseResult::Success(TestTypeId::Val1));
    }

    #[test]
    fn test_parse_type_id_invalid() {
        let mut buf = vec![];

        buf.put_u8(3);

        let (_, result) = parse_type_id::<TestTypeId>(&buf).expect("Should succeed");

        assert_eq!(result, TypeParseResult::Failure(3));
    }
}
