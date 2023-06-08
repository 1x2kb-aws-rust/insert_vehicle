use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
#[serde(rename_all(serialize = "camelCase"))]
pub struct InsertCompletedMessage {
    pub id: Option<String>,
    pub success: bool,
    pub messages: Vec<String>,
    pub errors: Vec<String>,
    pub warnings: Vec<String>,
}
