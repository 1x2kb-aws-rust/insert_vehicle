mod vehicle;

use aws_lambda_events::event::sns::SnsEvent;
use lambda_runtime::{run, service_fn, Error, LambdaEvent};

/// This is the main body for the function.
/// Write your code inside it.
/// There are some code example in the following URLs:
/// - https://github.com/awslabs/aws-lambda-rust-runtime/tree/main/examples
/// - https://github.com/aws-samples/serverless-rust-demo/
async fn function_handler(event: LambdaEvent<SnsEvent>) -> Result<(), Error> {
    let (sns_event, _context) = event.into_parts();

    lib::execute(sns_event.records).await.unwrap();

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

/*
"postgres://{}:{}@{}:{}/{}",
            self.get_username(),
            self.get_password(),
            self.get_host(),
            self.get_port(),
            self.get_database_name()
 */
