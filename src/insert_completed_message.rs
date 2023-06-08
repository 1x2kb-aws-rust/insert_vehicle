use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, PartialEq)]
#[serde(rename_all(serialize = "camelCase"))]
pub struct InsertCompletedMessage {
    pub id: String,
    pub success: bool,
    pub messages: Vec<String>,
    pub errors: Vec<String>,
    pub warnings: Vec<String>,
}
