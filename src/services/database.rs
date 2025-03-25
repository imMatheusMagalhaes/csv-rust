use anyhow::Result;
use mongodb::{Client, Collection, options::ClientOptions};
use serde_json::Value;
use std::env;
use std::error::Error;

pub async fn create_mongo_client() -> Result<Client> {
    let url = env::var("MONGO_URL").unwrap_or(String::from("mongodb://localhost:27017"));
    let client = Client::with_options(ClientOptions::parse(&url).await?)?;
    Ok(client)
}

pub async fn insert_batch_to_mongo(
    collection: &Collection<Value>,
    batch: Vec<Value>,
) -> Result<(), Box<dyn Error>> {
    if !batch.is_empty() {
        collection.insert_many(batch, None).await?;
    }
    Ok(())
}

pub async fn get_collection(mongo_client: &Client) -> Result<Collection<Value>> {
    let name = env::var("DATABASE_NAME").unwrap_or(String::from("csv_database"));
    let collection = env::var("COLLECTION_NAME").unwrap_or(String::from("records"));
    let db = mongo_client.database(&name);
    let collection = db.collection::<Value>(&collection);
    Ok(collection)
}
