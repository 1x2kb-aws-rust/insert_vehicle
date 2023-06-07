use base64::{engine::general_purpose, Engine};
use mongodb::{
    options::{ClientOptions, ServerApi, ServerApiVersion},
    Client, Database,
};
use vehicle::Vehicle;

mod event;
mod vehicle;

/*
   Steps:
       Recieve records as Vec<SnsRecord>
       for each:
           parse message & message attributes separately (independently unit testable)
           message:
               base64 decode
               serialize to rust types
               insert data into database
               return result
           message attributes:
               convert from HashMap into known event::MessageAttributes
               return result
           after both:
               receive tupple of (SQL Insert Result, event::MessageAttributes)
               Create insert_vehicle_completed events
               Send insert_vehicle_completed events for each insert_vehicle we recieved -- even errors.
*/

fn base_64_decode(base_64: String) -> Result<Vec<u8>, base64::DecodeError> {
    general_purpose::STANDARD.decode(base_64)
}

fn stringify(decoded: Vec<u8>) -> Result<String, std::string::FromUtf8Error> {
    String::from_utf8(decoded)
}

fn serialize_vehicle(json_string: String) -> Result<Vehicle, serde_json::Error> {
    serde_json::from_str(&json_string)
}

async fn get_database(mongo_uri: String) -> mongodb::error::Result<Database> {
    let mut client_options = ClientOptions::parse(mongo_uri).await?;
    let server_api = ServerApi::builder().version(ServerApiVersion::V1).build();
    client_options.server_api = Some(server_api);
    Client::with_options(client_options).map(|client| client.database("main"))
}

#[cfg(test)]
const DECODED_UTF: [u8; 81] = [
    123, 34, 109, 97, 107, 101, 34, 58, 32, 34, 67, 104, 101, 118, 114, 111, 108, 101, 116, 34, 44,
    32, 34, 109, 111, 100, 101, 108, 34, 58, 32, 34, 83, 105, 108, 118, 101, 114, 97, 100, 111, 34,
    44, 32, 34, 109, 111, 100, 101, 108, 95, 121, 101, 97, 114, 34, 58, 32, 34, 50, 48, 50, 50, 34,
    44, 32, 118, 105, 110, 58, 32, 34, 48, 49, 50, 51, 52, 54, 55, 34, 125,
];
#[cfg(test)]
const PURE_STRING: &str = "{\"make\": \"Chevrolet\", \"model\": \"Silverado\", \"model_year\": \"2022\", vin: \"0123467\"}";
#[cfg(test)]
const PROPERLY_FORMATTED_JSON: &str = "{\"make\": \"Chevrolet\", \"model\": \"Silverado\", \"modelYear\": \"2022\", \"vin\": \"0123467\"}";

#[cfg(test)]
mod base_64_should {
    use base64::{engine::general_purpose, Engine};

    use crate::{base_64_decode, DECODED_UTF, PURE_STRING};

    #[test]
    fn decode_valid_string() {
        let mut encoded = String::new();
        general_purpose::STANDARD.encode_string(PURE_STRING, &mut encoded);

        let expected = DECODED_UTF;

        let result = base_64_decode(encoded).unwrap();
        assert_eq!(result, expected);
    }

    // It's hard for humans to look at the test above and know that it's working correctly. Here's an extra test to compare strings which humans can understand!
    #[test]
    fn decode_valid_string_with_matching_string() {
        let mut encoded = String::new();
        general_purpose::STANDARD.encode_string(PURE_STRING, &mut encoded);

        let result = base_64_decode(encoded)
            .map(String::from_utf8)
            .unwrap()
            .unwrap();
        assert_eq!(result, PURE_STRING);
    }

    #[test]
    fn not_decode_invalid_string() {
        let result = base_64_decode(PURE_STRING.to_string());

        assert!(
            result.is_err(),
            "base_64_decode parsed correctly but should have been an error"
        );
    }
}

#[cfg(test)]
mod stringify_should {
    use crate::{stringify, DECODED_UTF, PURE_STRING};

    #[test]
    fn should_stringify_valid_value() {
        let expected = PURE_STRING.to_string();

        let result = stringify(DECODED_UTF.into_iter().collect()).unwrap();

        assert_eq!(result, expected);
    }

    #[test]
    fn should_return_error_in_invalid() {
        // How do I make this fail? Type checking makes this hard to force a failure.
    }
}

#[cfg(test)]
mod serialize_should {
    use crate::{serialize_vehicle, vehicle::Vehicle, PROPERLY_FORMATTED_JSON, PURE_STRING};

    #[test]
    fn serialize_valid_vehicle_json() {
        let expected = Vehicle {
            make: "Chevrolet".to_string(),
            model: "Silverado".to_string(),
            model_year: "2022".to_string(),
            vin: "0123467".to_string(),
        };

        let result = serialize_vehicle(PROPERLY_FORMATTED_JSON.to_string()).unwrap();

        assert_eq!(result, expected);
    }

    #[test]
    fn fails_to_parse_incomple_json() {
        let incomplete_json =
            "{\"make\": \"Chevrolet\", \"model\": \"Silverado\", \"model_year\": \"2022\"}"
                .to_string();

        let result = serialize_vehicle(incomplete_json);

        assert!(
            result.is_err(),
            "Serialized incomplete_json but it shouldn't have"
        );
    }

    #[test]
    fn fails_to_parse_snake_case_key() {
        let result = serialize_vehicle(PURE_STRING.to_string());

        assert!(result.is_err(), "Serialized JSON string but shouldn't have");
    }
}

#[cfg(test)]
mod get_database_should {
    use super::get_database;

    #[tokio::test]
    #[ignore]
    async fn establishes_connection() {
        dotenvy::dotenv().ok();

        let value = get_database(std::env::var("MONGO_URI").expect("No MONGO_URI was set")).await;

        assert!(value.is_ok());
    }

    #[tokio::test]
    #[ignore]
    async fn not_establish_connection() {
        let value = get_database("some_random_string".to_string()).await;

        assert!(value.is_err())
    }
}
