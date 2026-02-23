use std::collections::HashMap;
use bytes::Bytes;
use parquet::file::reader::{FileReader, SerializedFileReader};
use parquet::file::metadata::ParquetMetaData;
use serde::{Deserialize, Serialize};
use parquet_lens_common::{ParquetLensError, Result};

/// parsed s3:// URI
#[derive(Debug, Clone)]
pub struct S3Uri {
    pub bucket: String,
    pub key: String,
}

pub fn parse_s3_uri(uri: &str) -> Option<S3Uri> {
    let stripped = uri.strip_prefix("s3://")?;
    let (bucket, key) = stripped.split_once('/')?;
    Some(S3Uri { bucket: bucket.to_owned(), key: key.to_owned() })
}

pub fn is_s3_uri(path: &str) -> bool {
    path.starts_with("s3://")
}

/// list all .parquet objects under s3://bucket/prefix using aws-sdk-s3
pub async fn list_s3_parquet(uri: &str) -> Result<Vec<String>> {
    let s3_uri = parse_s3_uri(uri).ok_or_else(|| ParquetLensError::Other(format!("invalid S3 URI: {uri}")))?;
    let config = aws_config::load_from_env().await;
    let client = aws_sdk_s3::Client::new(&config);
    let mut keys = Vec::new();
    let mut paginator = client
        .list_objects_v2()
        .bucket(&s3_uri.bucket)
        .prefix(&s3_uri.key)
        .into_paginator()
        .send();
    while let Some(page) = paginator.next().await {
        let page = page.map_err(|e| ParquetLensError::Other(e.to_string()))?;
        for obj in page.contents() {
            if let Some(k) = obj.key() {
                if k.ends_with(".parquet") {
                    keys.push(format!("s3://{}/{}", s3_uri.bucket, k));
                }
            }
        }
    }
    Ok(keys)
}

/// read Parquet footer from S3 using HTTP Range requests (task 43)
pub async fn read_s3_parquet_metadata(uri: &str, endpoint_url: Option<&str>) -> Result<ParquetMetaData> {
    let bytes = fetch_s3_bytes(uri, endpoint_url).await?;
    let reader = SerializedFileReader::new(bytes).map_err(ParquetLensError::Parquet)?;
    Ok(reader.metadata().clone())
}

/// fetch full object bytes (for now; selective range-read requires custom ChunkReader)
async fn fetch_s3_bytes(uri: &str, endpoint_url: Option<&str>) -> Result<Bytes> {
    let s3_uri = parse_s3_uri(uri).ok_or_else(|| ParquetLensError::Other(format!("invalid S3 URI: {uri}")))?;
    let mut config_loader = aws_config::load_from_env().await;
    let mut builder = aws_sdk_s3::config::Builder::from(&config_loader);
    if let Some(ep) = endpoint_url {
        builder = builder.endpoint_url(ep);
    }
    let client = aws_sdk_s3::Client::from_conf(builder.build());
    let resp = client
        .get_object()
        .bucket(&s3_uri.bucket)
        .key(&s3_uri.key)
        .send()
        .await
        .map_err(|e| ParquetLensError::Other(e.to_string()))?;
    let data = resp.body.collect().await
        .map_err(|e| ParquetLensError::Other(e.to_string()))?;
    Ok(data.into_bytes())
}

/// selective column chunk read via S3 range request (task 44)
/// returns bytes for specified byte range [start, end)
pub async fn read_s3_range(uri: &str, start: i64, end: i64, endpoint_url: Option<&str>) -> Result<Bytes> {
    let s3_uri = parse_s3_uri(uri).ok_or_else(|| ParquetLensError::Other(format!("invalid S3 URI: {uri}")))?;
    let config = aws_config::load_from_env().await;
    let mut builder = aws_sdk_s3::config::Builder::from(&config);
    if let Some(ep) = endpoint_url { builder = builder.endpoint_url(ep); }
    let client = aws_sdk_s3::Client::from_conf(builder.build());
    let range_header = format!("bytes={start}-{}", end - 1);
    let resp = client
        .get_object()
        .bucket(&s3_uri.bucket)
        .key(&s3_uri.key)
        .range(range_header)
        .send()
        .await
        .map_err(|e| ParquetLensError::Other(e.to_string()))?;
    let data = resp.body.collect().await
        .map_err(|e| ParquetLensError::Other(e.to_string()))?;
    Ok(data.into_bytes())
}
