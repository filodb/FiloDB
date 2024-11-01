//! Utilites for testing

use crate::field_constants;
use tantivy::{
    schema::{
        Field, JsonObjectOptions, Schema, SchemaBuilder, TextFieldIndexing, FAST, INDEXED, STORED,
        STRING,
    },
    Index, Searcher, TantivyDocument,
};

pub const COL1_NAME: &str = "col1";
pub const COL2_NAME: &str = "col2";
pub const JSON_COL_NAME: &str = "json_col";
pub const JSON_ATTRIBUTE1_NAME: &str = "f1";
pub const JSON_ATTRIBUTE2_NAME: &str = "f2";

pub struct TestIndex {
    pub schema: Schema,
    pub searcher: Searcher,
    pub json_field: Field,
}

// Allow unwraps since this is test code
#[allow(clippy::unwrap_used)]
pub fn build_test_schema() -> TestIndex {
    let mut builder = SchemaBuilder::new();

    builder.add_text_field(COL1_NAME, STRING | FAST);
    builder.add_text_field(COL2_NAME, STRING | FAST);
    builder.add_i64_field(field_constants::PART_ID, INDEXED | FAST);
    builder.add_i64_field(field_constants::START_TIME, INDEXED | FAST);
    builder.add_i64_field(field_constants::END_TIME, INDEXED | FAST);
    builder.add_bytes_field(field_constants::PART_KEY, INDEXED | FAST | STORED);
    builder.add_json_field(
        JSON_COL_NAME,
        JsonObjectOptions::default()
            .set_indexing_options(TextFieldIndexing::default().set_tokenizer("raw"))
            .set_fast(Some("raw")),
    );

    let schema = builder.build();

    let index = Index::create_in_ram(schema.clone());

    {
        let mut writer = index.writer::<TantivyDocument>(50_000_000).unwrap();

        let doc = TantivyDocument::parse_json(
            &schema,
            r#"{
                "col1": "ABC",
                "col2": "def",
                "__partIdDv__": 1,
                "__startTime__": 1234,
                "__endTime__": 1235,
                "__partKey__": "QUE=",
                "json_col": {
                    "f1": "value",
                    "f2": "value2"
                }
            }"#,
        )
        .unwrap();

        writer.add_document(doc).unwrap();

        let doc = TantivyDocument::parse_json(
            &schema,
            r#"{
                "col1": "DEF",
                "col2": "abc",
                "__partIdDv__": 10,
                "__startTime__": 4321,
                "__endTime__": 10000,
                "__partKey__": "QkI=",
                "json_col": {
                    "f1": "othervalue",
                    "f2": "othervalue2"
                }
            }"#,
        )
        .unwrap();

        writer.add_document(doc).unwrap();

        writer.commit().unwrap();
    }

    let reader = index.reader().unwrap();
    let searcher = reader.searcher();

    let json_field = schema.get_field(JSON_COL_NAME).unwrap();

    TestIndex {
        schema,
        searcher,
        json_field,
    }
}
