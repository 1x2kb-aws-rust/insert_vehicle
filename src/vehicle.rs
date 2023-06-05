use serde::Deserialize;

#[derive(Debug, Deserialize)]
#[serde(rename_all(deserialize = "camelCase"))]
pub struct Vehicle {
    pub make: String,
    pub model: String,
    pub model_year: String,
    pub vin: String,
    pub dln: String,
}
