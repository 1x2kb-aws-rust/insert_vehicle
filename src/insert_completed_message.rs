use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, PartialEq)]
#[serde(rename_all(serialize = "camelCase"))]
pub struct InsertCompletedMessage {
    id: String,
    success: bool,
    messages: Vec<String>,
    errors: Vec<String>,
    warnings: Vec<String>,
}
