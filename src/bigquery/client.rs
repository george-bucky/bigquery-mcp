use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use gcp_auth::TokenProvider;
use reqwest::StatusCode;
use serde::{Deserialize, Serialize};
use tokio::sync::OnceCell;
use tokio::sync::Semaphore;
use tokio::time::sleep;

use crate::bigquery::BigQueryBackend;
use crate::config::Config;
use crate::error::{AppError, Result};
use crate::sql;
use crate::types::{
    ColumnItem, DatasetDescriptionItem, DatasetItem, DatasetPage, GetDatasetDescriptionOutput,
    QueryHistoryItem, QueryParam, TableItem,
};

const SCOPE_CLOUD_PLATFORM: &str = "https://www.googleapis.com/auth/cloud-platform";
const MAX_RETRIES: u32 = 3;

#[derive(Clone)]
pub struct BigQueryClient {
    http: reqwest::Client,
    auth_provider: Arc<OnceCell<Arc<dyn TokenProvider>>>,
    config: Config,
    concurrency_limiter: Arc<Semaphore>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct DatasetsListResponse {
    datasets: Option<Vec<DatasetListItem>>,
    next_page_token: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct DatasetListItem {
    dataset_reference: DatasetReference,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct DatasetReference {
    dataset_id: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct DatasetDetailsResponse {
    #[serde(default)]
    description: Option<String>,
    #[serde(default)]
    creation_time: Option<String>,
    #[serde(default)]
    last_modified_time: Option<String>,
    #[serde(default)]
    location: Option<String>,
    dataset_reference: DatasetReference,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct JobsQueryRequest {
    query: String,
    use_legacy_sql: bool,
    location: String,
    parameter_mode: String,
    query_parameters: Vec<JobsQueryParameter>,
    timeout_ms: u64,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct JobsQueryParameter {
    name: String,
    parameter_type: JobsQueryParameterType,
    parameter_value: JobsQueryParameterValue,
}

#[derive(Debug, Serialize)]
struct JobsQueryParameterType {
    #[serde(rename = "type")]
    typ: String,
}

#[derive(Debug, Serialize)]
struct JobsQueryParameterValue {
    #[serde(skip_serializing_if = "Option::is_none")]
    value: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct JobsQueryResponse {
    #[serde(default)]
    job_complete: bool,
    #[serde(default)]
    schema: Option<QuerySchema>,
    #[serde(default)]
    rows: Option<Vec<QueryRow>>,
    #[serde(default)]
    page_token: Option<String>,
    #[serde(default)]
    job_reference: Option<JobReference>,
}

#[derive(Debug, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
struct JobReference {
    job_id: String,
    #[serde(default)]
    location: Option<String>,
}

#[derive(Debug, Deserialize)]
struct QuerySchema {
    fields: Vec<QueryField>,
}

#[derive(Debug, Deserialize)]
struct QueryField {
    name: String,
}

#[derive(Debug, Deserialize)]
struct QueryRow {
    f: Vec<QueryCell>,
}

#[derive(Debug, Deserialize)]
struct QueryCell {
    v: serde_json::Value,
}

impl BigQueryClient {
    pub async fn new(config: Config) -> Result<Self> {
        let http = reqwest::Client::builder()
            .timeout(config.query_timeout)
            .build()
            .map_err(|err| AppError::Transport(format!("failed to build http client: {err}")))?;

        Ok(Self {
            http,
            auth_provider: Arc::new(OnceCell::new()),
            concurrency_limiter: Arc::new(Semaphore::new(config.max_concurrency)),
            config,
        })
    }

    async fn auth_provider(&self) -> Result<Arc<dyn TokenProvider>> {
        let provider = self
            .auth_provider
            .get_or_try_init(|| async {
                gcp_auth::provider().await.map_err(|err| {
                    AppError::Unauthenticated(format!(
                        "failed to initialize ADC authentication: {err}"
                    ))
                })
            })
            .await?;
        Ok(provider.clone())
    }

    async fn bearer_token(&self) -> Result<String> {
        let provider = self.auth_provider().await?;
        let token = provider
            .token(&[SCOPE_CLOUD_PLATFORM])
            .await
            .map_err(|err| {
                AppError::Unauthenticated(format!("failed to get access token: {err}"))
            })?;
        Ok(token.as_str().to_string())
    }

    async fn send_get<T>(&self, url: &str, query: &[(String, String)]) -> Result<T>
    where
        T: for<'de> Deserialize<'de>,
    {
        let _permit = self
            .concurrency_limiter
            .acquire()
            .await
            .map_err(|err| AppError::Internal(format!("semaphore closed: {err}")))?;

        let mut attempt = 0;
        loop {
            attempt += 1;
            let token = self.bearer_token().await?;
            let response = self
                .http
                .get(url)
                .bearer_auth(token)
                .query(query)
                .send()
                .await
                .map_err(|err| classify_reqwest_error(err, "GET", url))?;

            if response.status().is_success() {
                let parsed = response.json::<T>().await.map_err(|err| {
                    AppError::Serialization(format!("failed to deserialize GET {url}: {err}"))
                })?;
                return Ok(parsed);
            }

            let status = response.status();
            let body = response
                .text()
                .await
                .unwrap_or_else(|_| "<no body>".to_string());
            let error = classify_status(status, &body);
            if should_retry_status(&error) && attempt < MAX_RETRIES {
                sleep(backoff_duration(attempt)).await;
                continue;
            }
            return Err(error);
        }
    }

    async fn send_post_json<T, U>(&self, url: &str, body: &U) -> Result<T>
    where
        T: for<'de> Deserialize<'de>,
        U: Serialize + ?Sized,
    {
        let _permit = self
            .concurrency_limiter
            .acquire()
            .await
            .map_err(|err| AppError::Internal(format!("semaphore closed: {err}")))?;

        let mut attempt = 0;
        loop {
            attempt += 1;
            let token = self.bearer_token().await?;
            let response = self
                .http
                .post(url)
                .bearer_auth(token)
                .json(body)
                .send()
                .await
                .map_err(|err| classify_reqwest_error(err, "POST", url))?;

            if response.status().is_success() {
                let parsed = response.json::<T>().await.map_err(|err| {
                    AppError::Serialization(format!("failed to deserialize POST {url}: {err}"))
                })?;
                return Ok(parsed);
            }

            let status = response.status();
            let body = response
                .text()
                .await
                .unwrap_or_else(|_| "<no body>".to_string());
            let error = classify_status(status, &body);
            if should_retry_status(&error) && attempt < MAX_RETRIES {
                sleep(backoff_duration(attempt)).await;
                continue;
            }
            return Err(error);
        }
    }

    async fn execute_query(
        &self,
        sql: String,
        parameters: Vec<QueryParam>,
    ) -> Result<Vec<HashMap<String, Option<String>>>> {
        let endpoint = format!(
            "{}/projects/{}/queries",
            self.config.api_base_url, self.config.project_id
        );

        let payload = JobsQueryRequest {
            query: sql,
            use_legacy_sql: false,
            location: self.config.region.clone(),
            parameter_mode: "NAMED".to_string(),
            query_parameters: parameters
                .into_iter()
                .map(|param| JobsQueryParameter {
                    name: param.name,
                    parameter_type: JobsQueryParameterType { typ: param.typ },
                    parameter_value: JobsQueryParameterValue { value: param.value },
                })
                .collect(),
            timeout_ms: self.config.query_timeout.as_millis() as u64,
        };

        let mut response = self
            .send_post_json::<JobsQueryResponse, _>(&endpoint, &payload)
            .await?;

        let mut rows = Vec::new();
        if response.job_complete {
            rows.extend(extract_rows(response.schema.as_ref(), response.rows.take()));
        }

        if !response.job_complete {
            let job_ref = response.job_reference.clone().ok_or_else(|| {
                AppError::Internal(
                    "BigQuery returned jobComplete=false without a job reference".to_string(),
                )
            })?;

            response = self.wait_for_job_completion(&job_ref).await?;
            rows.extend(extract_rows(response.schema.as_ref(), response.rows.take()));
        }

        let job_ref = response.job_reference.clone();
        let mut page_token = response.page_token;
        while let Some(token) = page_token {
            let reference = job_ref.as_ref().ok_or_else(|| {
                AppError::Internal(
                    "missing job reference while paginating query results".to_string(),
                )
            })?;
            let mut next = self.get_query_results_page(reference, Some(token)).await?;
            rows.extend(extract_rows(next.schema.as_ref(), next.rows.take()));
            page_token = next.page_token.take();
        }

        Ok(rows)
    }

    async fn wait_for_job_completion(&self, job_ref: &JobReference) -> Result<JobsQueryResponse> {
        let mut attempts = 0;
        loop {
            attempts += 1;
            let page = self.get_query_results_page(job_ref, None).await?;
            if page.job_complete {
                return Ok(page);
            }
            if attempts >= 10 {
                return Err(AppError::UpstreamTimeout(
                    "query did not complete within polling window".to_string(),
                ));
            }
            sleep(Duration::from_millis(200 * attempts as u64)).await;
        }
    }

    async fn get_query_results_page(
        &self,
        job_ref: &JobReference,
        page_token: Option<String>,
    ) -> Result<JobsQueryResponse> {
        let location = job_ref
            .location
            .as_deref()
            .unwrap_or(self.config.region.as_str())
            .to_string();

        let endpoint = format!(
            "{}/projects/{}/queries/{}",
            self.config.api_base_url, self.config.project_id, job_ref.job_id
        );

        let mut query = vec![
            (
                "timeoutMs".to_string(),
                (self.config.query_timeout.as_millis() as u64).to_string(),
            ),
            ("location".to_string(), location),
        ];

        if let Some(token) = page_token {
            query.push(("pageToken".to_string(), token));
        }

        self.send_get::<JobsQueryResponse>(&endpoint, &query).await
    }

    fn map_rows_to_dataset_descriptions(
        rows: Vec<HashMap<String, Option<String>>>,
    ) -> Vec<DatasetDescriptionItem> {
        rows.into_iter()
            .filter_map(|row| {
                let dataset = row.get("dataset").and_then(Clone::clone)?;
                let description = row.get("description").and_then(Clone::clone)?;
                Some(DatasetDescriptionItem {
                    dataset,
                    description,
                })
            })
            .collect()
    }

    fn map_rows_to_tables(rows: Vec<HashMap<String, Option<String>>>) -> Vec<TableItem> {
        rows.into_iter()
            .filter_map(|row| {
                let relation = row.get("relation").and_then(Clone::clone)?;
                let relation_type = row.get("relation_type").and_then(Clone::clone)?;
                Some(TableItem {
                    relation,
                    relation_type,
                    description: row.get("description").and_then(Clone::clone),
                    created_at: row.get("created_at").and_then(Clone::clone),
                    last_modified_at: row.get("last_modified_at").and_then(Clone::clone),
                })
            })
            .collect()
    }

    fn map_rows_to_columns(rows: Vec<HashMap<String, Option<String>>>) -> Vec<ColumnItem> {
        rows.into_iter()
            .filter_map(|row| {
                let column = row.get("column").and_then(Clone::clone)?;
                let field_path = row.get("field_path").and_then(Clone::clone)?;
                let data_type = row.get("data_type").and_then(Clone::clone)?;
                Some(ColumnItem {
                    column,
                    field_path,
                    data_type,
                    description: row.get("description").and_then(Clone::clone),
                })
            })
            .collect()
    }

    fn map_rows_to_query_history(
        rows: Vec<HashMap<String, Option<String>>>,
    ) -> Vec<QueryHistoryItem> {
        rows.into_iter()
            .filter_map(|row| {
                let job_id = row.get("job_id").and_then(Clone::clone)?;
                let creation_time = row.get("creation_time").and_then(Clone::clone)?;
                let query = row.get("query").and_then(Clone::clone)?;
                Some(QueryHistoryItem {
                    job_id,
                    creation_time,
                    query,
                })
            })
            .collect()
    }
}

#[async_trait]
impl BigQueryBackend for BigQueryClient {
    async fn get_datasets(&self, limit: u32, cursor: Option<String>) -> Result<DatasetPage> {
        let endpoint = format!(
            "{}/projects/{}/datasets",
            self.config.api_base_url, self.config.project_id
        );

        let mut query = vec![("maxResults".to_string(), limit.to_string())];
        if let Some(token) = cursor {
            query.push(("pageToken".to_string(), token));
        }

        let response: DatasetsListResponse = self.send_get(&endpoint, &query).await?;

        let datasets = response
            .datasets
            .unwrap_or_default()
            .into_iter()
            .map(|item| DatasetItem {
                dataset_id: item.dataset_reference.dataset_id,
            })
            .collect();

        Ok(DatasetPage {
            datasets,
            next_cursor: response.next_page_token,
        })
    }

    async fn get_all_dataset_descriptions(
        &self,
        limit: u32,
        offset: u32,
    ) -> Result<Vec<DatasetDescriptionItem>> {
        let sql = sql::datasets_descriptions_sql(&self.config.region);
        let params = vec![
            QueryParam::int64("limit", i64::from(limit)),
            QueryParam::int64("offset", i64::from(offset)),
        ];
        let rows = self.execute_query(sql, params).await?;
        Ok(Self::map_rows_to_dataset_descriptions(rows))
    }

    async fn get_dataset_description(
        &self,
        dataset_id: &str,
    ) -> Result<GetDatasetDescriptionOutput> {
        let endpoint = format!(
            "{}/projects/{}/datasets/{}",
            self.config.api_base_url, self.config.project_id, dataset_id
        );

        let response: DatasetDetailsResponse = self.send_get(&endpoint, &[]).await?;

        Ok(GetDatasetDescriptionOutput {
            dataset_id: response.dataset_reference.dataset_id,
            description: response.description,
            created_at: response
                .creation_time
                .and_then(|value| epoch_millis_to_rfc3339(&value)),
            last_modified_at: response
                .last_modified_time
                .and_then(|value| epoch_millis_to_rfc3339(&value)),
            location: response.location,
        })
    }

    async fn get_tables(
        &self,
        dataset: &str,
        limit: u32,
        offset: u32,
        include_without_description: bool,
    ) -> Result<Vec<TableItem>> {
        let sql = sql::tables_sql(&self.config.project_id, dataset, &self.config.region);
        let params = vec![
            QueryParam::int64("limit", i64::from(limit)),
            QueryParam::int64("offset", i64::from(offset)),
            QueryParam::bool("include_without_description", include_without_description),
        ];
        let rows = self.execute_query(sql, params).await?;
        Ok(Self::map_rows_to_tables(rows))
    }

    async fn get_columns(
        &self,
        dataset: &str,
        table: &str,
        include_undocumented: bool,
    ) -> Result<Vec<ColumnItem>> {
        let sql = sql::columns_sql(&self.config.project_id, dataset);
        let params = vec![
            QueryParam::string("table", table),
            QueryParam::bool("include_undocumented", include_undocumented),
        ];
        let rows = self.execute_query(sql, params).await?;
        Ok(Self::map_rows_to_columns(rows))
    }

    async fn get_query_history(
        &self,
        dataset: &str,
        table: &str,
        limit: u32,
        lookback_days: u32,
        sample_mode: &str,
    ) -> Result<Vec<QueryHistoryItem>> {
        let sql = sql::jobs_sql(&self.config.region, sample_mode)?;
        let params = vec![
            QueryParam::string("project_id", &self.config.project_id),
            QueryParam::string("dataset_id", dataset),
            QueryParam::string("table_id", table),
            QueryParam::int64("limit", i64::from(limit)),
            QueryParam::int64("lookback_days", i64::from(lookback_days)),
            QueryParam::nullable_string("email_domain", self.config.human_email_domain.clone()),
        ];
        let rows = self.execute_query(sql, params).await?;
        Ok(Self::map_rows_to_query_history(rows))
    }
}

fn extract_rows(
    schema: Option<&QuerySchema>,
    rows: Option<Vec<QueryRow>>,
) -> Vec<HashMap<String, Option<String>>> {
    let Some(schema) = schema else {
        return Vec::new();
    };
    let Some(rows) = rows else {
        return Vec::new();
    };

    rows.into_iter()
        .map(|row| {
            let mut output = HashMap::new();
            for (index, field) in schema.fields.iter().enumerate() {
                let value = row
                    .f
                    .get(index)
                    .map(|cell| json_value_to_string(&cell.v))
                    .unwrap_or(None);
                output.insert(field.name.clone(), value);
            }
            output
        })
        .collect()
}

fn json_value_to_string(value: &serde_json::Value) -> Option<String> {
    match value {
        serde_json::Value::Null => None,
        serde_json::Value::String(v) => Some(v.clone()),
        _ => Some(value.to_string()),
    }
}

fn classify_reqwest_error(err: reqwest::Error, method: &str, url: &str) -> AppError {
    if err.is_timeout() {
        AppError::UpstreamTimeout(format!("{method} {url} timed out: {err}"))
    } else {
        AppError::Transport(format!("{method} {url} failed: {err}"))
    }
}

fn classify_status(status: StatusCode, body: &str) -> AppError {
    match status {
        StatusCode::UNAUTHORIZED => AppError::Unauthenticated(body.to_string()),
        StatusCode::FORBIDDEN => AppError::PermissionDenied(body.to_string()),
        StatusCode::NOT_FOUND => AppError::NotFound(body.to_string()),
        StatusCode::TOO_MANY_REQUESTS => AppError::RateLimited(body.to_string()),
        StatusCode::REQUEST_TIMEOUT | StatusCode::GATEWAY_TIMEOUT => {
            AppError::UpstreamTimeout(body.to_string())
        }
        s if s.is_server_error() => AppError::Transport(body.to_string()),
        _ => AppError::InvalidArgument(body.to_string()),
    }
}

fn should_retry_status(error: &AppError) -> bool {
    matches!(
        error,
        AppError::RateLimited(_) | AppError::UpstreamTimeout(_) | AppError::Transport(_)
    )
}

fn backoff_duration(attempt: u32) -> Duration {
    Duration::from_millis(100 * u64::from(2_u32.saturating_pow(attempt.saturating_sub(1))))
}

fn epoch_millis_to_rfc3339(value: &str) -> Option<String> {
    let millis = value.parse::<i64>().ok()?;
    let seconds = millis / 1000;
    let nanos = (millis % 1000) * 1_000_000;
    let dt = DateTime::<Utc>::from_timestamp(seconds, nanos as u32)?;
    Some(dt.to_rfc3339())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_epoch_millis_conversion() {
        let value = epoch_millis_to_rfc3339("0").expect("valid timestamp");
        assert!(value.starts_with("1970-01-01T00:00:00"));
    }

    #[test]
    fn test_extract_rows() {
        let schema = QuerySchema {
            fields: vec![
                QueryField {
                    name: "dataset".to_string(),
                },
                QueryField {
                    name: "description".to_string(),
                },
            ],
        };

        let row = QueryRow {
            f: vec![
                QueryCell {
                    v: serde_json::Value::String("a".to_string()),
                },
                QueryCell {
                    v: serde_json::Value::Null,
                },
            ],
        };

        let rows = extract_rows(Some(&schema), Some(vec![row]));
        assert_eq!(rows.len(), 1);
        assert_eq!(
            rows[0].get("dataset").and_then(Clone::clone),
            Some("a".to_string())
        );
        assert_eq!(rows[0].get("description").and_then(Clone::clone), None);
    }
}
