mod services;
mod helpers;
use services::s3::create_client;
use services::queue::create_channel;
use services::queue::process_csv_from_queue;
use services::database::create_mongo_client;
use services::database::get_collection;
use std::error::Error;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let s3_client = create_client().await?;
    let mongo_client = create_mongo_client().await?;
    let collection = get_collection(&mongo_client).await?;
    let channel = create_channel().await?;
    println!("Iniciando serviço");
    process_csv_from_queue(&channel, &s3_client, &collection).await?;
    Ok(())
}
