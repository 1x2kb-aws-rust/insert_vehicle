use aws_lambda_events::event::sns::SnsEvent;
use base64::{engine::general_purpose, Engine};
use lambda_runtime::{run, service_fn, Error, LambdaEvent};

/// This is the main body for the function.
/// Write your code inside it.
/// There are some code example in the following URLs:
/// - https://github.com/awslabs/aws-lambda-rust-runtime/tree/main/examples
/// - https://github.com/aws-samples/serverless-rust-demo/
async fn function_handler(event: LambdaEvent<SnsEvent>) -> Result<(), Error> {
    let (sns_event, _context) = event.into_parts();
    let data: Vec<String> = sns_event
        .records
        .into_iter()
        .map(|base_64_vehicle| {
            general_purpose::STANDARD
                .decode(base_64_vehicle.sns.message)
        })
        .flatten()
        .map(String::from_utf8)
        .flatten()
        .collect();

    println!("{:#?}", data);
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Error> {
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        // disable printing the name of the module in every log line.
        .with_target(false)
        // disabling time is handy because CloudWatch will add the ingestion time.
        .without_time()
        .init();

    run(service_fn(function_handler)).await
}
