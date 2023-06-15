use aws_lambda_events::sns::{MessageAttribute, SnsRecord};
use aws_sdk_sns::{
    config::Region,
    error::SdkError,
    operation::publish::{PublishError, PublishOutput},
    Client as SnsClient,
};
use base64::{engine::general_purpose, Engine};
use event::MessageAttributes;
use insert_completed_message::InsertCompletedMessage;
// use mongodb::{
//     options::{ClientOptions, ServerApi, ServerApiVersion},
//     Client as MongoClient, Collection, Database,
// };
use serde_json::Error;
use std::collections::HashMap;
use uuid::Uuid;
use vehicle::Vehicle;

mod event;
mod insert_completed_message;
mod vehicle;

pub async fn execute(sns_records: Vec<SnsRecord>) -> Result<(), ()> {
    // TODO: Too complex, rewrite.
    // Get serialize and decode event as sent
    let vehicle_items: Vec<(Result<Vehicle, String>, MessageAttributes)> = sns_records
        .into_iter()
        .map(|sns_record| {
            let vehicle_result =
                deserialize_vehicle(sns_record.sns.message).map_err(|e| e.to_string());

            (
                vehicle_result,
                MessageAttributes::from(sns_record.sns.message_attributes),
            )
        })
        .collect();
    println!("Successfully deserialized messages");

    let mut vehicles: Vec<Vehicle> = Vec::new();
    let mut attributes: Vec<MessageAttributes> = Vec::new();
    // For those that have errored send an error message. For those that have not
    // add them to the vectors above to do a bulk insert.
    for (vehicle_result, source_message_atributes) in vehicle_items.into_iter() {
        match vehicle_result {
            Ok(vehicle) => {
                vehicles.push(vehicle);
                attributes.push(source_message_atributes)
            }
            Err(e) => {
                println!("Error occurred while deserializing error {}", e);
                send_error_event(e, &source_message_atributes).await;
            }
        }
    }

    println!("Finished sending errors (if any)");

    println!("Handling vehicles");
    let serialized_vehicles = insert_vehicles_temp(vehicles.iter());
    println!("Finished processing vehicles");

    for i in 0..serialized_vehicles.len() {
        match serialized_vehicles.get(i).unwrap() {
            Ok(vehicle_str) => {
                send_completed_message(vehicle_str, attributes.get(i).unwrap()).await;
            }
            Err(error) => {
                println!("Experienced error while serializing vehicle");
                println!("{}", error);
            }
        }
    }

    Ok(())
}

async fn send_completed_message(vehicle_str: &str, source_attributes: &MessageAttributes) {
    let (message, message_attributes) =
        create_message_and_attributes(vehicle_str, source_attributes);

    match send_insert_completed(
        get_client(get_region()).await,
        get_topic(),
        message,
        message_attributes,
    )
    .await
    {
        Ok(published_output) => {
            println!(
                "successfully published message {}",
                published_output.message_id().unwrap_or_default()
            );
        }
        Err(_) => {
            println!("Failed to send message");
        }
    };
}

fn create_message_and_attributes(
    vehicle: &str,
    source_attributes: &MessageAttributes,
) -> (String, MessageAttributes) {
    let message = InsertCompletedMessage {
        id: Some(Uuid::new_v4().to_string()),
        success: true,
        payload: Some(vehicle.to_string()),
        messages: Vec::new(),
        errors: Vec::new(),
        warnings: Vec::new(),
    };

    let message_attributes =
        create_insert_completed_attributes(message.id.clone(), source_attributes);

    let message = serialize_insert_completed(message).unwrap();

    println!(
        "Preparing to send message ({},{}) in response to message ({},{})",
        &message_attributes.event_id,
        &message_attributes.event_type,
        message_attributes
            .source_event_id
            .clone()
            .unwrap_or_default(),
        message_attributes
            .source_event_type
            .clone()
            .unwrap_or_default()
    );

    (message, message_attributes)
}

async fn send_error_event(error: String, source_message_atributes: &MessageAttributes) {
    let message_attributes = create_insert_completed_attributes(None, source_message_atributes);

    let message = serialize_insert_completed(InsertCompletedMessage {
        id: None,
        success: false,
        messages: Vec::new(),
        errors: Vec::from([error]),
        warnings: Vec::new(),
        payload: None,
    })
    .map_err(|e| e.to_string())
    .map(|json_string| base_64_encode(&json_string))
    .unwrap();

    send_insert_completed(
        get_client(get_region()).await,
        get_topic(),
        message,
        message_attributes,
    )
    .await
    .map(|publish_output| {
        println!(
            "published with message id: {}",
            publish_output.message_id().unwrap_or("")
        );

        ()
    })
    .unwrap_or(());
}

fn base_64_decode(base_64: String) -> Result<Vec<u8>, base64::DecodeError> {
    general_purpose::STANDARD.decode(base_64)
}

fn stringify(decoded: Vec<u8>) -> Result<String, std::string::FromUtf8Error> {
    String::from_utf8(decoded)
}

fn deserialize_vehicle(json_string: String) -> Result<Vehicle, serde_json::Error> {
    serde_json::from_str(&json_string)
}

fn insert_vehicles_temp<'a>(
    vehicles: impl Iterator<Item = &'a Vehicle>,
) -> Vec<Result<String, Error>> {
    vehicles
        .into_iter()
        .map(|vehicle| serde_json::to_string(vehicle))
        .collect()
}

// async fn get_database(mongo_uri: String) -> mongodb::error::Result<Database> {
//     let mut client_options = ClientOptions::parse(mongo_uri).await?;
//     let server_api = ServerApi::builder().version(ServerApiVersion::V1).build();
//     client_options.server_api = Some(server_api);
//     MongoClient::with_options(client_options).map(|client| client.database("main"))
// }

// async fn insert_vehicles(
//     vehicle: impl Iterator<Item = &Vehicle>,
//     collection: Collection<Vehicle>,
// ) -> mongodb::error::Result<Vec<String>> {
//     let start = Instant::now();

//     println!("Beginning insert of vehicles");
//     let r = collection.insert_many(vehicle, None).await.map(|map| {
//         map.inserted_ids
//             .into_values()
//             .map(|bson| bson.to_string())
//             .collect()
//     });

//     let duration = start.elapsed();
//     println!("{:?}", duration);

//     r
// }

fn parse_message_attributes(
    attributes: HashMap<String, MessageAttribute>,
) -> event::MessageAttributes {
    event::MessageAttributes::from(attributes)
}

fn create_insert_completed_attributes(
    inserted_vehicle_id: Option<String>,
    source_event: &MessageAttributes,
) -> MessageAttributes {
    MessageAttributes {
        event_id: Uuid::new_v4().to_string(),
        event_type: "insert_vehicle_completed".to_string(),
        resource_id: inserted_vehicle_id,
        source_event_id: Some(source_event.event_id.to_string()),
        source_event_type: Some(source_event.event_type.to_string()),
    }
}

fn serialize_insert_completed(
    message: InsertCompletedMessage,
) -> Result<String, serde_json::Error> {
    serde_json::to_string(&message)
}

fn get_region() -> Region {
    std::env::var("REGION")
        .map(Region::new)
        .unwrap_or(Region::new("us-east-2"))
}

// Untested
async fn get_client(region: Region) -> SnsClient {
    let shared_config = aws_config::from_env().region(region).load().await;
    let client = SnsClient::new(&shared_config);

    client
}

fn get_topic() -> String {
    std::env::var("TOPIC_ARN").unwrap_or_default()
}

fn base_64_encode(s: &str) -> String {
    let mut buffer = String::new();
    general_purpose::STANDARD.encode_string(s, &mut buffer);

    buffer
}

async fn send_insert_completed(
    client: SnsClient,
    topic: String,
    serialized_insert_completed: String,
    message_attributes: MessageAttributes,
) -> Result<PublishOutput, SdkError<PublishError>> {
    client
        .publish()
        .topic_arn(topic)
        .message(serialized_insert_completed)
        .set_message_attributes(Some(message_attributes.to_map()))
        .send()
        .await
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
mod deserialize_should {
    use crate::{deserialize_vehicle, vehicle::Vehicle, PROPERLY_FORMATTED_JSON, PURE_STRING};

    #[test]
    fn serialize_valid_vehicle_json() {
        let expected = Vehicle {
            make: "Chevrolet".to_string(),
            model: "Silverado".to_string(),
            model_year: "2022".to_string(),
            vin: "0123467".to_string(),
        };

        let result = deserialize_vehicle(PROPERLY_FORMATTED_JSON.to_string()).unwrap();

        assert_eq!(result, expected);
    }

    #[test]
    fn fails_to_parse_incomple_json() {
        let incomplete_json =
            "{\"make\": \"Chevrolet\", \"model\": \"Silverado\", \"model_year\": \"2022\"}"
                .to_string();

        let result = deserialize_vehicle(incomplete_json);

        assert!(
            result.is_err(),
            "Serialized incomplete_json but it shouldn't have"
        );
    }

    #[test]
    fn fails_to_parse_snake_case_key() {
        let result = deserialize_vehicle(PURE_STRING.to_string());

        assert!(result.is_err(), "Serialized JSON string but shouldn't have");
    }
}

// #[cfg(test)]
// mod get_database_should {
//     use super::get_database;

//     #[tokio::test]
//     #[ignore]
//     async fn establishes_connection() {
//         dotenvy::dotenv().ok();

//         let value = get_database(std::env::var("MONGO_URI").expect("No MONGO_URI was set")).await;

//         assert!(value.is_ok());
//     }

//     #[tokio::test]
//     #[ignore]
//     async fn not_establish_connection() {
//         let value = get_database("some_random_string".to_string()).await;

//         assert!(value.is_err())
//     }
// }

// #[cfg(test)]
// mod insert_vehicle_should {
//     use dotenvy::dotenv;

//     use crate::{
//         get_database, insert_vehicles,
//         vehicle::{Random, Vehicle},
//     };

//     #[tokio::test]
//     #[ignore]
//     async fn insert_random_vehicles() {
//         dotenv().ok();

//         let vehicles: Vec<Vehicle> = (0..2).into_iter().map(|_| Vehicle::random()).collect();

//         let collection =
//             get_database(std::env::var("MONGO_URI").expect("Failed to find MONGO_URI env"))
//                 .await
//                 .expect("Failed to establish a db connection")
//                 .collection::<Vehicle>("test_vehicle");

//         let insert_many_result = insert_vehicles(vehicles.iter(), collection).await.unwrap();

//         assert_eq!(insert_many_result.len(), vehicles.len());
//     }
// }

#[cfg(test)]
mod parse_message_attributes_should {

    use std::collections::HashMap;

    use aws_lambda_events::sns::MessageAttribute;

    use crate::{event::MessageAttributes, parse_message_attributes};

    #[test]
    fn parse_message_attributes_from_map() {
        let map = create_attribute_map();
        let map_clone = map.clone();

        let expected = MessageAttributes {
            event_id: map
                .get("eventId")
                .map(|c| c.value.to_string())
                .unwrap_or_default(),
            event_type: map
                .get("eventType")
                .map(|c| c.value.to_string())
                .unwrap_or_default(),
            resource_id: map.get("resourceId").map(|c| c.value.to_string()),
            source_event_id: map.get("sourceEventId").map(|c| c.value.to_string()),
            source_event_type: map.get("sourceEventType").map(|c| c.value.to_string()),
        };

        let message_attributes = parse_message_attributes(map_clone);
        assert_eq!(message_attributes, expected);
    }

    #[test]
    fn parses_when_no_source_id() {
        let mut map = create_attribute_map();
        map.remove("sourceEventId").unwrap();
        let map_clone = map.clone();

        let expected = MessageAttributes {
            event_id: map
                .get("eventId")
                .map(|c| c.value.to_string())
                .unwrap_or_default(),
            event_type: map
                .get("eventType")
                .map(|c| c.value.to_string())
                .unwrap_or_default(),
            resource_id: map.get("resourceId").map(|c| c.value.to_string()),
            source_event_id: None,
            source_event_type: map.get("sourceEventType").map(|c| c.value.to_string()),
        };

        let message_attributes = parse_message_attributes(map_clone);

        assert_eq!(message_attributes, expected);
    }

    fn create_attribute_map() -> HashMap<String, MessageAttribute> {
        let event_id = "test_id".to_string();
        let event_type = "test_type".to_string();
        let resource_id = "test_resource_id".to_string();
        let source_event_id = "test_source_event_id".to_string();
        let source_event_type = "test_source_event_type".to_string();
        let source_event_domain = "test_source_event_domain".to_string();

        let mut map: HashMap<String, MessageAttribute> = HashMap::new();
        map.insert(
            "eventId".to_string(),
            create_message_attribute(event_id.clone()),
        );

        map.insert(
            "eventType".to_string(),
            create_message_attribute(event_type.clone()),
        );

        map.insert(
            "resourceId".to_string(),
            create_message_attribute(resource_id.clone()),
        );

        map.insert(
            "sourceEventId".to_string(),
            create_message_attribute(source_event_id.clone()),
        );

        map.insert(
            "sourceEventType".to_string(),
            create_message_attribute(source_event_type.clone()),
        );

        map.insert(
            "sourceEventDomain".to_string(),
            create_message_attribute(source_event_domain.clone()),
        );

        map
    }

    fn create_message_attribute(value: String) -> MessageAttribute {
        MessageAttribute {
            data_type: "String".to_string(),
            value,
        }
    }
}

#[cfg(test)]
mod create_insert_completed_attributes_should {
    use uuid::Uuid;

    use crate::{create_insert_completed_attributes, event::MessageAttributes};

    #[test]
    fn some_function() {
        let insert_id = Uuid::new_v4().to_string();
        let insert_vehicle = MessageAttributes {
            event_id: Uuid::new_v4().to_string(),
            event_type: "insert_vehicle_requested".to_string(),
            resource_id: None,
            source_event_id: None,
            source_event_type: None,
        };

        let message_attributes =
            create_insert_completed_attributes(Some(insert_id.to_string()), &insert_vehicle);
        let expected = MessageAttributes {
            event_id: message_attributes.event_id.to_string(),
            event_type: "insert_vehicle_completed".to_string(),
            resource_id: message_attributes.resource_id.clone(),
            source_event_id: Some(insert_vehicle.event_id.to_string()),
            source_event_type: Some(insert_vehicle.event_type.to_string()),
        };
        assert_eq!(message_attributes, expected);
    }
}

#[cfg(test)]
mod serialize_insert_completed_should {
    use crate::{insert_completed_message::InsertCompletedMessage, serialize_insert_completed};

    #[test]
    fn serialize_the_message_happy_path() {
        let message = InsertCompletedMessage {
            id: Some("test_id".to_string()),
            success: true,
            messages: Vec::from(["successfully inserted vehicle".to_string()]),
            errors: Vec::new(),
            warnings: Vec::new(),
            payload: None,
        };
        let expected = "{\"id\":\"test_id\",\"success\":true,\"payload\":null,\"messages\":[\"successfully inserted vehicle\"],\"errors\":[],\"warnings\":[]}".to_string();

        let result = serialize_insert_completed(message.clone()).unwrap();

        assert_eq!(result, expected);
    }

    #[test]
    fn serialize_the_message_as_error() {
        let message = InsertCompletedMessage {
            id: None,
            success: false,
            messages: Vec::new(),
            errors: Vec::from(["Couldn't establish connection to the database".to_string()]),
            warnings: Vec::new(),
            payload: None,
        };

        let expected ="{\"id\":null,\"success\":false,\"payload\":null,\"messages\":[],\"errors\":[\"Couldn't establish connection to the database\"],\"warnings\":[]}".to_string();

        let result = serialize_insert_completed(message.clone()).unwrap();

        assert_eq!(result, expected);
    }

    #[test]
    fn serialize_the_message_with_warning() {
        let message = InsertCompletedMessage {
            id: Some("test_id".to_string()),
            success: true,
            messages: Vec::new(),
            errors: Vec::new(),
            warnings: Vec::from(["Car with exact same information already inserted".to_string()]),
            payload: None,
        };

        let expected = "{\"id\":\"test_id\",\"success\":true,\"payload\":null,\"messages\":[],\"errors\":[],\"warnings\":[\"Car with exact same information already inserted\"]}".to_string();

        let result = serialize_insert_completed(message.clone()).unwrap();

        assert_eq!(result, expected);
    }

    #[test]
    fn error_when() {
        // TODO: How do I make this fail? Strong type checking makes this difficult
    }
}

#[cfg(test)]
mod get_region_should {
    use crate::get_region;

    #[test]
    fn extract_region_from_env() {
        let region = "test-region".to_string();

        std::env::set_var("REGION", &region);

        let result = get_region();

        assert_eq!(result.to_string(), region);
    }

    #[test]
    fn default_to_us_east_2() {
        let expected = "us-east-2";

        let result = get_region();

        assert_eq!(result.to_string(), expected);
    }
}

#[cfg(test)]
mod get_topic_should {
    use crate::get_topic;

    #[test]
    fn return_topic_from_env() {
        let topic = "test_topic".to_string();
        std::env::set_var("TOPIC_ARN", &topic);
        let result = get_topic();

        assert_eq!(result, topic);
    }

    // Update, should not default but throw an error instead.
    // Using default for now
    // Sometimes fails because tests are run in parallel and the above test sets TOPIC_ARN.
    #[test]
    fn return_empty_string_as_default() {
        let result = get_topic();

        assert_eq!(result, "".to_string());
    }
}

#[cfg(test)]
mod base_64_encode_should {
    use crate::{base_64_decode, base_64_encode, stringify};

    const EXAMPLE_STR: &str = "{\"id\": null, \"success\": false, \"messages\": [], \"warnings\": [],\"errors\": [\"Failed to encode string, sent fallback string\"]}";

    #[test]
    fn encode_string() {
        let expected = "eyJpZCI6IG51bGwsICJzdWNjZXNzIjogZmFsc2UsICJtZXNzYWdlcyI6IFtdLCAid2FybmluZ3MiOiBbXSwiZXJyb3JzIjogWyJGYWlsZWQgdG8gZW5jb2RlIHN0cmluZywgc2VudCBmYWxsYmFjayBzdHJpbmciXX0=".to_string();

        let result = base_64_encode(EXAMPLE_STR);

        assert_eq!(result, expected);
    }

    #[test]
    fn encodes_something_decodeable_by_standard() {
        let result = base_64_encode(EXAMPLE_STR);

        let decoded = stringify(base_64_decode(result.clone()).unwrap()).unwrap();

        assert_eq!(decoded, EXAMPLE_STR.to_string());
    }
}

#[cfg(test)]
mod send_insert_completed_should {
    use uuid::Uuid;

    use crate::{
        event::MessageAttributes, get_client, get_region, get_topic, send_insert_completed,
    };

    #[tokio::test]
    #[ignore]
    async fn send_message() {
        dotenvy::dotenv().ok();

        let message = "{\"id\":\"test_id\",\"success\":true,\"messages\":[\"successfully inserted vehicle\"],\"errors\":[],\"warnings\":[]}".to_string();
        let message_attributes = MessageAttributes {
            event_id: Uuid::new_v4().to_string(),
            event_type: "test_insert_vehicle_completed".to_string(),
            resource_id: Some(Uuid::new_v4().to_string()),
            source_event_id: Some(Uuid::new_v4().to_string()),
            source_event_type: Some("test_insert_vehicle_requested".to_string()),
        };

        let result = send_insert_completed(
            get_client(get_region()).await,
            get_topic(),
            message,
            message_attributes,
        )
        .await;

        assert!(result.is_ok());
    }

    #[tokio::test]
    #[ignore]
    async fn fail_when_topic_is_invalid() {
        let message = "{\"id\":\"test_id\",\"success\":true,\"messages\":[\"successfully inserted vehicle\"],\"errors\":[],\"warnings\":[]}".to_string();
        let message_attributes = MessageAttributes {
            event_id: Uuid::new_v4().to_string(),
            event_type: "test_insert_vehicle_completed".to_string(),
            resource_id: Some(Uuid::new_v4().to_string()),
            source_event_id: Some(Uuid::new_v4().to_string()),
            source_event_type: Some("test_insert_vehicle_requested".to_string()),
        };

        let result = send_insert_completed(
            get_client(get_region()).await,
            "".to_string(),
            message,
            message_attributes,
        )
        .await;

        assert!(result.is_err());
    }
}
