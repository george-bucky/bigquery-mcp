pub mod client;

use async_trait::async_trait;

use crate::error::Result;
use crate::types::{
    ColumnItem, DatasetDescriptionItem, DatasetPage, GetDatasetDescriptionOutput, QueryHistoryItem,
    TableItem,
};

#[async_trait]
pub trait BigQueryBackend: Send + Sync {
    async fn get_datasets(&self, limit: u32, cursor: Option<String>) -> Result<DatasetPage>;

    async fn get_all_dataset_descriptions(
        &self,
        limit: u32,
        offset: u32,
    ) -> Result<Vec<DatasetDescriptionItem>>;

    async fn get_dataset_description(
        &self,
        dataset_id: &str,
    ) -> Result<GetDatasetDescriptionOutput>;

    async fn get_tables(
        &self,
        dataset: &str,
        limit: u32,
        offset: u32,
        include_without_description: bool,
    ) -> Result<Vec<TableItem>>;

    async fn get_columns(
        &self,
        dataset: &str,
        table: &str,
        include_undocumented: bool,
    ) -> Result<Vec<ColumnItem>>;

    async fn get_query_history(
        &self,
        dataset: &str,
        table: &str,
        limit: u32,
        lookback_days: u32,
        sample_mode: &str,
    ) -> Result<Vec<QueryHistoryItem>>;
}
