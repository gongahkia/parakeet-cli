use bytes::Bytes;
use parquet::file::reader::{FileReader, SerializedFileReader};
use parquet::file::metadata::ParquetMetaData;
use parquet_lens_common::{ParquetLensError, Result};

/// parsed gs:// URI
#[derive(Debug, Clone)]
pub struct GcsUri {
    pub bucket: String,
    pub object: String,
}

pub fn parse_gcs_uri(uri: &str) -> Option<GcsUri> {
    let stripped = uri.strip_prefix("gs://")?;
    let (bucket, object) = stripped.split_once('/')?;
    Some(GcsUri { bucket: bucket.to_owned(), object: object.to_owned() })
}

pub fn is_gcs_uri(path: &str) -> bool {
    path.starts_with("gs://")
}

/// list .parquet objects under gs://bucket/prefix via GCS JSON API
/// uses application default credentials (GOOGLE_APPLICATION_CREDENTIALS or metadata server)
pub async fn list_gcs_parquet(uri: &str) -> Result<Vec<String>> {
    let gcs_uri = parse_gcs_uri(uri).ok_or_else(|| ParquetLensError::Other(format!("invalid GCS URI: {uri}")))?;
    let token = get_adc_token().await?;
    let url = format!(
        "https://storage.googleapis.com/storage/v1/b/{}/o?prefix={}&fields=items/name",
        gcs_uri.bucket, gcs_uri.object
    );
    let client = reqwest::Client::new();
    let resp = client.get(&url)
        .bearer_auth(&token)
        .send().await
        .map_err(|e| ParquetLensError::Other(e.to_string()))?
        .json::<serde_json::Value>().await
        .map_err(|e| ParquetLensError::Other(e.to_string()))?;
    let mut keys = Vec::new();
    if let Some(items) = resp.get("items").and_then(|v| v.as_array()) {
        for item in items {
            if let Some(name) = item.get("name").and_then(|v| v.as_str()) {
                if name.ends_with(".parquet") {
                    keys.push(format!("gs://{}/{}", gcs_uri.bucket, name));
                }
            }
        }
    }
    Ok(keys)
}

/// read Parquet metadata from GCS object
pub async fn read_gcs_parquet_metadata(uri: &str) -> Result<ParquetMetaData> {
    let bytes = fetch_gcs_bytes(uri).await?;
    let reader = SerializedFileReader::new(bytes).map_err(ParquetLensError::Parquet)?;
    Ok(reader.metadata().clone())
}

async fn fetch_gcs_bytes(uri: &str) -> Result<Bytes> {
    let gcs_uri = parse_gcs_uri(uri).ok_or_else(|| ParquetLensError::Other(format!("invalid GCS URI: {uri}")))?;
    let token = get_adc_token().await?;
    let url = format!(
        "https://storage.googleapis.com/storage/v1/b/{}/o/{}?alt=media",
        gcs_uri.bucket,
        urlencoded(&gcs_uri.object)
    );
    let client = reqwest::Client::new();
    let resp = client.get(&url)
        .bearer_auth(&token)
        .send().await
        .map_err(|e| ParquetLensError::Other(e.to_string()))?;
    let status = resp.status();
    if status == reqwest::StatusCode::UNAUTHORIZED || status == reqwest::StatusCode::FORBIDDEN {
        return Err(ParquetLensError::Auth(format!("GCS returned HTTP {status} for {uri}")));
    }
    let bytes = resp.bytes().await.map_err(|e| ParquetLensError::Other(e.to_string()))?;
    Ok(bytes)
}

/// fetch application default credentials token from metadata server or env
async fn get_adc_token() -> Result<String> {
    // check GOOGLE_APPLICATION_CREDENTIALS env — minimal implementation uses metadata server
    let client = reqwest::Client::new();
    let url = "http://metadata.google.internal/computeMetadata/v1/instance/service-accounts/default/token";
    let resp = client.get(url)
        .header("Metadata-Flavor", "Google")
        .send().await
        .map_err(|e| ParquetLensError::Other(format!("ADC token fetch failed: {e}")))?
        .json::<serde_json::Value>().await
        .map_err(|e| ParquetLensError::Other(e.to_string()))?;
    resp.get("access_token")
        .and_then(|v| v.as_str())
        .map(|s| s.to_owned())
        .ok_or_else(|| ParquetLensError::Other("no access_token in ADC response".into()))
}

fn urlencoded(s: &str) -> String {
    s.replace('/', "%2F")
}
