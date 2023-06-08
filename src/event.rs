use std::collections::HashMap;

use aws_lambda_events::sns::MessageAttribute;
use aws_sdk_sns::types::MessageAttributeValue;

#[derive(Debug, PartialEq)]
pub struct MessageAttributes {
    pub event_id: String,
    pub event_type: String,
    pub resource_id: Option<String>,
    pub source_event_id: Option<String>,
    pub source_event_type: Option<String>,
}

impl From<HashMap<String, MessageAttribute>> for MessageAttributes {
    fn from(value: HashMap<String, MessageAttribute>) -> Self {
        let event_id = value
            .get("eventId")
            .map(|message_attribute| message_attribute.value.to_string())
            .unwrap_or_default();

        let event_type = value
            .get("eventType")
            .map(|message_attribute| message_attribute.value.to_string())
            .unwrap_or_default();

        let resource_id = value
            .get("resourceId")
            .map(|message_attribute| message_attribute.value.to_string());

        let source_event_id = value
            .get("sourceEventId")
            .map(|message_attribute| message_attribute.value.to_string());

        let source_event_type = value
            .get("sourceEventType")
            .map(|message_attribute| message_attribute.value.to_string());

        Self {
            event_id,
            event_type,
            resource_id,
            source_event_id,
            source_event_type,
        }
    }
}

impl MessageAttributes {
    pub fn new(
        event_id: String,
        event_type: String,
        resource_id: Option<String>,
        source_event_id: Option<String>,
        source_event_type: Option<String>,
    ) -> Self {
        Self {
            event_id,
            event_type,
            resource_id,
            source_event_id,
            source_event_type,
        }
    }

    pub fn to_map(&self) -> HashMap<String, MessageAttributeValue> {
        let mut map = HashMap::new();

        map.insert(
            "eventId".to_string(),
            MessageAttributeValue::builder()
                .data_type("String".to_string())
                .string_value(self.event_id.to_string())
                .build(),
        );
        map.insert(
            "eventType".to_string(),
            MessageAttributeValue::builder()
                .data_type("String".to_string())
                .string_value(self.event_type.to_string())
                .build(),
        );

        self.add_optional_keys(&mut map);

        map
    }

    fn add_optional_keys(&self, map: &mut HashMap<String, MessageAttributeValue>) {
        if let Some(source_id) = &self.source_event_id {
            map.insert(
                "sourceEventId".to_string(),
                MessageAttributeValue::builder()
                    .data_type("String".to_string())
                    .string_value(source_id.to_string())
                    .build(),
            );
        }

        if let Some(source_type) = &self.source_event_type {
            map.insert(
                "sourceEventType".to_string(),
                MessageAttributeValue::builder()
                    .data_type("String".to_string())
                    .string_value(source_type.to_string())
                    .build(),
            );
        }

        if let Some(resource_id) = &self.resource_id {
            map.insert(
                "resourceId".to_string(),
                MessageAttributeValue::builder()
                    .data_type("String".to_string())
                    .string_value(resource_id.to_string())
                    .build(),
            );
        }
    }

    pub fn set_source_properites(&mut self, source_message: &MessageAttributes) {
        self.source_event_id = Some(source_message.event_id.to_string());
        self.source_event_type = Some(source_message.event_type.to_string());
    }
}
