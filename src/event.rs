use std::collections::HashMap;

use aws_sdk_sns::types::MessageAttributeValue;
use uuid::Uuid;

pub struct MessageAttributes {
    event_id: Uuid,
    event_type: String,
    resource_id: Option<String>,
    source_event_id: Option<String>,
    source_event_type: Option<String>,
    source_event_domain: Option<String>,
}

impl MessageAttributes {
    pub fn new(
        event_id: Uuid,
        event_type: String,
        resource_id: Option<String>,
        source_event_id: Option<String>,
        source_event_type: Option<String>,
        source_event_domain: Option<String>,
    ) -> Self {
        Self {
            event_id,
            event_type,
            resource_id,
            source_event_id,
            source_event_type,
            source_event_domain,
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

        if let Some(source_domain) = &self.source_event_domain {
            map.insert(
                "sourceEventDomain".to_string(),
                MessageAttributeValue::builder()
                    .data_type("String".to_string())
                    .string_value(source_domain.to_string())
                    .build(),
            );
        }
    }
}
