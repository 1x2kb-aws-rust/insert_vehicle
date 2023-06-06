mod vehicle;
mod event;



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
