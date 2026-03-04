use serde::{Deserialize, Serialize};

pub fn default_limit_10() -> u32 {
    10
}

pub fn default_limit_200() -> u32 {
    200
}

pub fn default_limit_500() -> u32 {
    500
}

pub fn default_lookback_days() -> u32 {
    7
}

pub fn default_sample_mode() -> String {
    "recent".to_string()
}

#[derive(Debug, Clone, Deserialize)]
pub struct GetDatasetsInput {
    #[serde(default = "default_limit_200")]
    pub limit: u32,
    #[serde(default)]
    pub cursor: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct DatasetItem {
    pub dataset_id: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct GetDatasetsOutput {
    pub datasets: Vec<DatasetItem>,
    pub next_cursor: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct GetAllDatasetDescriptionsInput {
    #[serde(default = "default_limit_500")]
    pub limit: u32,
    #[serde(default)]
    pub cursor: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct DatasetDescriptionItem {
    pub dataset: String,
    pub description: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct GetAllDatasetDescriptionsOutput {
    pub datasets: Vec<DatasetDescriptionItem>,
    pub next_cursor: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct GetDatasetDescriptionInput {
    pub dataset_id: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct GetDatasetDescriptionOutput {
    pub dataset_id: String,
    pub description: Option<String>,
    pub created_at: Option<String>,
    pub last_modified_at: Option<String>,
    pub location: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct GetTablesInput {
    pub dataset: String,
    #[serde(default = "default_limit_500")]
    pub limit: u32,
    #[serde(default)]
    pub cursor: Option<String>,
    #[serde(default)]
    pub include_without_description: bool,
}

#[derive(Debug, Clone, Serialize)]
pub struct TableItem {
    pub relation: String,
    pub relation_type: String,
    pub description: Option<String>,
    pub created_at: Option<String>,
    pub last_modified_at: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct GetTablesOutput {
    pub tables: Vec<TableItem>,
    pub next_cursor: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct GetColumnsInput {
    pub dataset: String,
    pub table: String,
    #[serde(default)]
    pub include_undocumented: bool,
}

#[derive(Debug, Clone, Serialize)]
pub struct ColumnItem {
    pub column: String,
    pub field_path: String,
    pub data_type: String,
    pub description: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct GetColumnsOutput {
    pub columns: Vec<ColumnItem>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct GetQueryHistoryInput {
    pub dataset: String,
    pub table: String,
    #[serde(default = "default_limit_10")]
    pub limit: u32,
    #[serde(default = "default_lookback_days")]
    pub lookback_days: u32,
    #[serde(default = "default_sample_mode")]
    pub sample_mode: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct QueryHistoryItem {
    pub job_id: String,
    pub creation_time: String,
    pub query: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct GetQueryHistoryOutput {
    pub queries: Vec<QueryHistoryItem>,
}

#[derive(Debug, Clone)]
pub struct DatasetPage {
    pub datasets: Vec<DatasetItem>,
    pub next_cursor: Option<String>,
}

#[derive(Debug, Clone)]
pub struct QueryParam {
    pub name: String,
    pub typ: String,
    pub value: Option<String>,
}

impl QueryParam {
    pub fn string(name: &str, value: impl Into<String>) -> Self {
        Self {
            name: name.to_string(),
            typ: "STRING".to_string(),
            value: Some(value.into()),
        }
    }

    pub fn int64(name: &str, value: i64) -> Self {
        Self {
            name: name.to_string(),
            typ: "INT64".to_string(),
            value: Some(value.to_string()),
        }
    }

    pub fn bool(name: &str, value: bool) -> Self {
        Self {
            name: name.to_string(),
            typ: "BOOL".to_string(),
            value: Some(if value { "true" } else { "false" }.to_string()),
        }
    }

    pub fn nullable_string(name: &str, value: Option<String>) -> Self {
        Self {
            name: name.to_string(),
            typ: "STRING".to_string(),
            value,
        }
    }
}
