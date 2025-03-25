use aws_sdk_s3::Client;
use aws_sdk_s3::primitives::ByteStream;
use aws_config::load_from_env;
use anyhow::{Result, Context};

pub async fn create_client() -> Result<Client> {
    let config = load_from_env().await;
    let client = Client::new(&config);
    Ok(client)
}

pub async fn get_file_stream(
    client: &Client,
    bucket: &str,
    file_name: &str,
) -> Result<ByteStream> {
    let response = client
        .get_object()
        .bucket(bucket)
        .key(file_name)
        .send()
        .await
        .context("Erro ao obter o arquivo do S3")?;

    Ok(response.body)
}