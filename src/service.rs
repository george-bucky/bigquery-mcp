use std::hash::{Hash, Hasher};
use std::sync::Arc;

use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;

use crate::bigquery::BigQueryBackend;
use crate::cache::TtlCache;
use crate::config::Config;
use crate::error::{AppError, Result};
use crate::types::{
    GetAllDatasetDescriptionsInput, GetAllDatasetDescriptionsOutput, GetColumnsInput,
    GetColumnsOutput, GetDatasetDescriptionInput, GetDatasetsInput, GetDatasetsOutput,
    GetQueryHistoryInput, GetQueryHistoryOutput, GetTablesInput, GetTablesOutput,
};
use crate::validation::validate_identifier;

const MAX_LIMIT: u32 = 1000;

#[derive(Clone)]
pub struct Service {
    backend: Arc<dyn BigQueryBackend>,
    datasets_cache: TtlCache<DatasetsKey, GetDatasetsOutput>,
    all_dataset_descriptions_cache:
        TtlCache<AllDatasetDescriptionsKey, GetAllDatasetDescriptionsOutput>,
    dataset_details_cache: TtlCache<String, crate::types::GetDatasetDescriptionOutput>,
    tables_cache: TtlCache<TablesKey, GetTablesOutput>,
    columns_cache: TtlCache<ColumnsKey, GetColumnsOutput>,
    query_history_cache: TtlCache<QueryHistoryKey, GetQueryHistoryOutput>,
}

#[derive(Debug, Clone, Eq)]
struct DatasetsKey {
    limit: u32,
    cursor: Option<String>,
}

impl PartialEq for DatasetsKey {
    fn eq(&self, other: &Self) -> bool {
        self.limit == other.limit && self.cursor == other.cursor
    }
}

impl Hash for DatasetsKey {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.limit.hash(state);
        self.cursor.hash(state);
    }
}

#[derive(Debug, Clone, Eq)]
struct AllDatasetDescriptionsKey {
    limit: u32,
    offset: u32,
}

impl PartialEq for AllDatasetDescriptionsKey {
    fn eq(&self, other: &Self) -> bool {
        self.limit == other.limit && self.offset == other.offset
    }
}

impl Hash for AllDatasetDescriptionsKey {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.limit.hash(state);
        self.offset.hash(state);
    }
}

#[derive(Debug, Clone, Eq)]
struct TablesKey {
    dataset: String,
    limit: u32,
    offset: u32,
    include_without_description: bool,
}

impl PartialEq for TablesKey {
    fn eq(&self, other: &Self) -> bool {
        self.dataset == other.dataset
            && self.limit == other.limit
            && self.offset == other.offset
            && self.include_without_description == other.include_without_description
    }
}

impl Hash for TablesKey {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.dataset.hash(state);
        self.limit.hash(state);
        self.offset.hash(state);
        self.include_without_description.hash(state);
    }
}

#[derive(Debug, Clone, Eq)]
struct ColumnsKey {
    dataset: String,
    table: String,
    include_undocumented: bool,
}

impl PartialEq for ColumnsKey {
    fn eq(&self, other: &Self) -> bool {
        self.dataset == other.dataset
            && self.table == other.table
            && self.include_undocumented == other.include_undocumented
    }
}

impl Hash for ColumnsKey {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.dataset.hash(state);
        self.table.hash(state);
        self.include_undocumented.hash(state);
    }
}

#[derive(Debug, Clone, Eq)]
struct QueryHistoryKey {
    dataset: String,
    table: String,
    limit: u32,
    lookback_days: u32,
    sample_mode: String,
}

impl PartialEq for QueryHistoryKey {
    fn eq(&self, other: &Self) -> bool {
        self.dataset == other.dataset
            && self.table == other.table
            && self.limit == other.limit
            && self.lookback_days == other.lookback_days
            && self.sample_mode == other.sample_mode
    }
}

impl Hash for QueryHistoryKey {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.dataset.hash(state);
        self.table.hash(state);
        self.limit.hash(state);
        self.lookback_days.hash(state);
        self.sample_mode.hash(state);
    }
}

impl Service {
    pub fn new(config: &Config, backend: Arc<dyn BigQueryBackend>) -> Self {
        Self {
            backend,
            datasets_cache: TtlCache::new(config.cache_ttl_datasets),
            all_dataset_descriptions_cache: TtlCache::new(config.cache_ttl_datasets),
            dataset_details_cache: TtlCache::new(config.cache_ttl_dataset_details),
            tables_cache: TtlCache::new(config.cache_ttl_tables),
            columns_cache: TtlCache::new(config.cache_ttl_columns),
            query_history_cache: TtlCache::new(config.cache_ttl_query_history),
        }
    }

    pub async fn get_datasets(&self, input: GetDatasetsInput) -> Result<GetDatasetsOutput> {
        let limit = normalize_limit(input.limit)?;
        let key = DatasetsKey {
            limit,
            cursor: input.cursor.clone(),
        };
        let backend = self.backend.clone();

        self.datasets_cache
            .get_or_try_insert_with(key, || async move {
                let page = backend.get_datasets(limit, input.cursor).await?;
                Ok(GetDatasetsOutput {
                    datasets: page.datasets,
                    next_cursor: page.next_cursor,
                })
            })
            .await
    }

    pub async fn get_all_dataset_descriptions(
        &self,
        input: GetAllDatasetDescriptionsInput,
    ) -> Result<GetAllDatasetDescriptionsOutput> {
        let limit = normalize_limit(input.limit)?;
        let offset = decode_offset_cursor(input.cursor.as_deref())?;
        let key = AllDatasetDescriptionsKey { limit, offset };
        let backend = self.backend.clone();

        self.all_dataset_descriptions_cache
            .get_or_try_insert_with(key, || async move {
                let rows = backend.get_all_dataset_descriptions(limit, offset).await?;
                let next_cursor = if rows.len() as u32 == limit {
                    Some(encode_offset_cursor(offset + limit))
                } else {
                    None
                };
                Ok(GetAllDatasetDescriptionsOutput {
                    datasets: rows,
                    next_cursor,
                })
            })
            .await
    }

    pub async fn get_dataset_description(
        &self,
        input: GetDatasetDescriptionInput,
    ) -> Result<crate::types::GetDatasetDescriptionOutput> {
        validate_identifier("dataset_id", &input.dataset_id)?;
        let key = input.dataset_id.clone();
        let backend = self.backend.clone();
        self.dataset_details_cache
            .get_or_try_insert_with(key, || async move {
                backend.get_dataset_description(&input.dataset_id).await
            })
            .await
    }

    pub async fn get_tables(&self, input: GetTablesInput) -> Result<GetTablesOutput> {
        validate_identifier("dataset", &input.dataset)?;
        let limit = normalize_limit(input.limit)?;
        let offset = decode_offset_cursor(input.cursor.as_deref())?;

        let key = TablesKey {
            dataset: input.dataset.clone(),
            limit,
            offset,
            include_without_description: input.include_without_description,
        };
        let backend = self.backend.clone();

        self.tables_cache
            .get_or_try_insert_with(key, || async move {
                let tables = backend
                    .get_tables(
                        &input.dataset,
                        limit,
                        offset,
                        input.include_without_description,
                    )
                    .await?;
                let next_cursor = if tables.len() as u32 == limit {
                    Some(encode_offset_cursor(offset + limit))
                } else {
                    None
                };
                Ok(GetTablesOutput {
                    tables,
                    next_cursor,
                })
            })
            .await
    }

    pub async fn get_columns(&self, input: GetColumnsInput) -> Result<GetColumnsOutput> {
        validate_identifier("dataset", &input.dataset)?;
        validate_identifier("table", &input.table)?;

        let key = ColumnsKey {
            dataset: input.dataset.clone(),
            table: input.table.clone(),
            include_undocumented: input.include_undocumented,
        };
        let backend = self.backend.clone();

        self.columns_cache
            .get_or_try_insert_with(key, || async move {
                let columns = backend
                    .get_columns(&input.dataset, &input.table, input.include_undocumented)
                    .await?;
                Ok(GetColumnsOutput { columns })
            })
            .await
    }

    pub async fn get_query_history(
        &self,
        input: GetQueryHistoryInput,
    ) -> Result<GetQueryHistoryOutput> {
        validate_identifier("dataset", &input.dataset)?;
        validate_identifier("table", &input.table)?;
        let limit = normalize_limit(input.limit)?;

        if input.lookback_days == 0 {
            return Err(AppError::InvalidArgument(
                "lookback_days must be at least 1".to_string(),
            ));
        }

        if !matches!(input.sample_mode.as_str(), "recent" | "stable_sample") {
            return Err(AppError::InvalidArgument(
                "sample_mode must be 'recent' or 'stable_sample'".to_string(),
            ));
        }

        let key = QueryHistoryKey {
            dataset: input.dataset.clone(),
            table: input.table.clone(),
            limit,
            lookback_days: input.lookback_days,
            sample_mode: input.sample_mode.clone(),
        };
        let backend = self.backend.clone();

        self.query_history_cache
            .get_or_try_insert_with(key, || async move {
                let queries = backend
                    .get_query_history(
                        &input.dataset,
                        &input.table,
                        limit,
                        input.lookback_days,
                        &input.sample_mode,
                    )
                    .await?;
                Ok(GetQueryHistoryOutput { queries })
            })
            .await
    }
}

fn normalize_limit(limit: u32) -> Result<u32> {
    if limit == 0 {
        return Err(AppError::InvalidArgument(
            "limit must be greater than 0".to_string(),
        ));
    }
    if limit > MAX_LIMIT {
        return Err(AppError::InvalidArgument(format!(
            "limit cannot exceed {MAX_LIMIT}"
        )));
    }
    Ok(limit)
}

fn encode_offset_cursor(offset: u32) -> String {
    URL_SAFE_NO_PAD.encode(offset.to_string())
}

fn decode_offset_cursor(cursor: Option<&str>) -> Result<u32> {
    let Some(cursor) = cursor else {
        return Ok(0);
    };

    let decoded = URL_SAFE_NO_PAD
        .decode(cursor)
        .map_err(|_| AppError::InvalidArgument("cursor is invalid base64".to_string()))?;

    let offset = String::from_utf8(decoded)
        .map_err(|_| AppError::InvalidArgument("cursor contains non-UTF8 bytes".to_string()))?
        .parse::<u32>()
        .map_err(|_| AppError::InvalidArgument("cursor is not a valid offset".to_string()))?;

    Ok(offset)
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use async_trait::async_trait;

    use super::*;
    use crate::bigquery::BigQueryBackend;
    use crate::types::{
        ColumnItem, DatasetDescriptionItem, DatasetItem, DatasetPage, GetDatasetDescriptionOutput,
        QueryHistoryItem, TableItem,
    };

    struct FakeBackend {
        datasets_calls: AtomicUsize,
    }

    #[async_trait]
    impl BigQueryBackend for FakeBackend {
        async fn get_datasets(&self, _limit: u32, _cursor: Option<String>) -> Result<DatasetPage> {
            self.datasets_calls.fetch_add(1, Ordering::SeqCst);
            Ok(DatasetPage {
                datasets: vec![DatasetItem {
                    dataset_id: "a".to_string(),
                }],
                next_cursor: None,
            })
        }

        async fn get_all_dataset_descriptions(
            &self,
            _limit: u32,
            _offset: u32,
        ) -> Result<Vec<DatasetDescriptionItem>> {
            Ok(vec![])
        }

        async fn get_dataset_description(
            &self,
            _dataset_id: &str,
        ) -> Result<GetDatasetDescriptionOutput> {
            Ok(GetDatasetDescriptionOutput {
                dataset_id: "a".to_string(),
                description: None,
                created_at: None,
                last_modified_at: None,
                location: None,
            })
        }

        async fn get_tables(
            &self,
            _dataset: &str,
            _limit: u32,
            _offset: u32,
            _include_without_description: bool,
        ) -> Result<Vec<TableItem>> {
            Ok(vec![])
        }

        async fn get_columns(
            &self,
            _dataset: &str,
            _table: &str,
            _include_undocumented: bool,
        ) -> Result<Vec<ColumnItem>> {
            Ok(vec![])
        }

        async fn get_query_history(
            &self,
            _dataset: &str,
            _table: &str,
            _limit: u32,
            _lookback_days: u32,
            _sample_mode: &str,
        ) -> Result<Vec<QueryHistoryItem>> {
            Ok(vec![])
        }
    }

    fn test_config() -> Config {
        Config {
            project_id: "project".to_string(),
            region: "europe-west2".to_string(),
            cache_ttl_datasets: std::time::Duration::from_secs(60),
            cache_ttl_dataset_details: std::time::Duration::from_secs(60),
            cache_ttl_tables: std::time::Duration::from_secs(60),
            cache_ttl_columns: std::time::Duration::from_secs(60),
            cache_ttl_query_history: std::time::Duration::from_secs(60),
            max_concurrency: 4,
            query_timeout: std::time::Duration::from_secs(15),
            human_email_domain: None,
            api_base_url: "http://localhost".to_string(),
        }
    }

    #[tokio::test]
    async fn test_get_datasets_cache_hit() {
        let backend = Arc::new(FakeBackend {
            datasets_calls: AtomicUsize::new(0),
        });
        let service = Service::new(&test_config(), backend.clone());

        let input = GetDatasetsInput {
            limit: 10,
            cursor: None,
        };

        let _ = service
            .get_datasets(input.clone())
            .await
            .expect("first call");
        let _ = service.get_datasets(input).await.expect("second call");

        assert_eq!(backend.datasets_calls.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn test_cursor_round_trip() {
        let encoded = encode_offset_cursor(50);
        let decoded = decode_offset_cursor(Some(&encoded)).expect("decode");
        assert_eq!(decoded, 50);
    }
}
