//! Field names

pub fn facet_field_name(name: &str) -> String {
    format!("{FACET_FIELD_PREFIX}{name}")
}

// These should be kept in sync with the constants in  PartKeyIndex.scala
// as they're fields that can be directly queried via incoming filters
// or fields that are filtered out of label lists
pub const DOCUMENT_ID: &str = "__partIdField__";
pub const PART_ID: &str = "__partIdDv__";
pub const PART_KEY: &str = "__partKey__";
pub const LABEL_LIST: &str = "__labelList__";
pub const FACET_FIELD_PREFIX: &str = "$facet_";
pub const START_TIME: &str = "__startTime__";
pub const END_TIME: &str = "__endTime__";
pub const TYPE: &str = "_type_";
