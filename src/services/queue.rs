use aws_sdk_s3::Client as S3_Client;
use csv_async::AsyncReaderBuilder;
use jsonschema::JSONSchema;
use mongodb::Collection;
use serde_json::{Value, json};
use std::error::Error;
use tokio_stream::StreamExt;

use lapin::{Channel, Connection, ConnectionProperties, options::*, types::FieldTable};
use serde::{Deserialize, Serialize};

use crate::services::{database::insert_batch_to_mongo, s3::get_file_stream};
use crate::helpers::profiling::get_rss_memory;

use std::env;
use std::time::Instant;

#[derive(Serialize, Deserialize, Debug)]
struct Request {
    file_name: String,
    bucket: String,
    schema: Value,
}

pub async fn create_channel() -> lapin::Result<Channel> {
    let url = env::var("RMQ_URL").unwrap_or(String::from("amqp://localhost:5672"));
    let connection = Connection::connect(&url, ConnectionProperties::default()).await?;
    let channel = connection.create_channel().await?;
    Ok(channel)
}

// pub async fn send_to_queue(channel: &Channel, msg: &Value, queue: &str) -> lapin::Result<()> {
//   let serialized_request = serde_json::to_string(msg).unwrap();

//   channel
//       .queue_declare(
//         queue,
//           QueueDeclareOptions::default(),
//           FieldTable::default(),
//       )
//       .await?;

//   channel
//       .basic_publish(
//           "",
//           queue,
//           BasicPublishOptions::default(),
//           serialized_request.as_bytes(),
//           BasicProperties::default(),
//       )
//       .await?
//       .await?;

//   Ok(())
// }

pub async fn process_csv_from_queue(
    channel: &Channel,
    s3_client: &S3_Client,
    collection: &Collection<Value>,
) -> Result<(), Box<dyn Error>> {
    channel
        .queue_declare(
            "request_queue",
            QueueDeclareOptions::default(),
            FieldTable::default(),
        )
        .await?;

    let mut consumer = channel
        .basic_consume(
            "request_queue",
            "consumer_tag",
            BasicConsumeOptions::default(),
            FieldTable::default(),
        )
        .await?;

    while let Some(delivery) = consumer.next().await {
        match delivery {
            Ok(delivery) => {
                let start = Instant::now();
                let initial_memory = get_rss_memory();

                let serialized_request = String::from_utf8_lossy(&delivery.data);
                println!("{}", serialized_request);
                let request: Request = serde_json::from_str(&serialized_request).unwrap();

                let compiled_schema = JSONSchema::compile(&request.schema).unwrap();

                let csv_stream =
                    get_file_stream(&s3_client, &request.bucket, &request.file_name).await?;

                let mut reader = AsyncReaderBuilder::new()
                    .create_reader(tokio::io::BufReader::new(csv_stream.into_async_read()));

                let headers = reader.headers().await?.clone();

                let mut batch = Vec::new();
                let batch_size = 500; // Inserção em lotes
                let mut lines = reader.into_records();

                while let Some(record) = lines.next().await {
                    let record = record?;
                    let mut json_record = serde_json::Map::new();

                    // Converte o CSV para JSON de forma dinâmica
                    for (i, field) in headers.iter().enumerate() {
                        if let Some(value) = record.get(i) {
                            json_record.insert(field.to_string(), json!(value));
                        }
                    }

                    let json_value = Value::Object(json_record);

                    // Valida o JSON com o Schema antes de salvar
                    if compiled_schema.validate(&json_value).is_ok() {
                        batch.push(json_value);
                    } else {
                        eprintln!("Falha na validação do JSON: {:?}", json_value);
                    }

                    if batch.len() >= batch_size {
                        if let Err(e) =
                            insert_batch_to_mongo(&collection, batch.drain(..).collect()).await
                        {
                            eprintln!("Erro ao inserir no MongoDB: {}", e);
                        }
                    }
                }

                // Inserir o restante dos registros
                if !batch.is_empty() {
                    insert_batch_to_mongo(&collection, batch).await?;
                }

                println!("CSV convertido e salvo no MongoDB!");

                let serialized_request = String::from_utf8_lossy(&delivery.data);
                let request: Request = serde_json::from_str(&serialized_request).unwrap();
                println!("Processando a solicitação: {:?}", request);
                channel
                    .basic_ack(delivery.delivery_tag, BasicAckOptions::default())
                    .await?;

                let final_memory = get_rss_memory();
                println!("Tempo de execução: {:?}", start.elapsed());
                println!("Memória utilizada: {} KB", final_memory - initial_memory);
            }
            Err(err) => {
                eprintln!("Erro ao consumir mensagem: {:?}", err);
            }
        }
    }
    Ok(())
}
