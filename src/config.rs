use std::collections::HashMap;
use std::env;
use std::time::Duration;

use crate::error::{AppError, Result};
use crate::validation::validate_region;

const DEFAULT_REGION: &str = "europe-west2";
const DEFAULT_TTL_DATASETS_SECS: u64 = 60;
const DEFAULT_TTL_DATASET_DETAILS_SECS: u64 = 300;
const DEFAULT_TTL_TABLES_SECS: u64 = 180;
const DEFAULT_TTL_COLUMNS_SECS: u64 = 300;
const DEFAULT_TTL_QUERY_HISTORY_SECS: u64 = 30;
const DEFAULT_MAX_CONCURRENCY: usize = 16;
const DEFAULT_QUERY_TIMEOUT_SECS: u64 = 15;
const DEFAULT_API_BASE_URL: &str = "https://bigquery.googleapis.com/bigquery/v2";

#[derive(Clone, Debug)]
pub struct Config {
    pub project_id: String,
    pub region: String,
    pub cache_ttl_datasets: Duration,
    pub cache_ttl_dataset_details: Duration,
    pub cache_ttl_tables: Duration,
    pub cache_ttl_columns: Duration,
    pub cache_ttl_query_history: Duration,
    pub max_concurrency: usize,
    pub query_timeout: Duration,
    pub human_email_domain: Option<String>,
    pub api_base_url: String,
}

impl Config {
    pub fn from_env() -> Result<Self> {
        let env_map = env::vars().collect::<HashMap<_, _>>();
        Self::from_map(&env_map)
    }

    pub fn from_map(map: &HashMap<String, String>) -> Result<Self> {
        let project_id = required(map, "BIGQUERY_PROJECT_ID")?;
        let region = optional(map, "BIGQUERY_REGION")
            .unwrap_or_else(|| DEFAULT_REGION.to_string())
            .to_lowercase();
        validate_region(&region)?;

        let cache_ttl_datasets = Duration::from_secs(parse_u64(
            map,
            "BQ_CACHE_TTL_DATASETS_SECS",
            DEFAULT_TTL_DATASETS_SECS,
        )?);
        let cache_ttl_dataset_details = Duration::from_secs(parse_u64(
            map,
            "BQ_CACHE_TTL_DATASET_DETAILS_SECS",
            DEFAULT_TTL_DATASET_DETAILS_SECS,
        )?);
        let cache_ttl_tables = Duration::from_secs(parse_u64(
            map,
            "BQ_CACHE_TTL_TABLES_SECS",
            DEFAULT_TTL_TABLES_SECS,
        )?);
        let cache_ttl_columns = Duration::from_secs(parse_u64(
            map,
            "BQ_CACHE_TTL_COLUMNS_SECS",
            DEFAULT_TTL_COLUMNS_SECS,
        )?);
        let cache_ttl_query_history = Duration::from_secs(parse_u64(
            map,
            "BQ_CACHE_TTL_QUERY_HISTORY_SECS",
            DEFAULT_TTL_QUERY_HISTORY_SECS,
        )?);

        let max_concurrency = parse_usize(map, "BQ_MAX_CONCURRENCY", DEFAULT_MAX_CONCURRENCY)?;
        if max_concurrency == 0 {
            return Err(AppError::InvalidArgument(
                "BQ_MAX_CONCURRENCY must be greater than 0".to_string(),
            ));
        }

        let query_timeout = Duration::from_secs(parse_u64(
            map,
            "BQ_QUERY_TIMEOUT_SECS",
            DEFAULT_QUERY_TIMEOUT_SECS,
        )?);

        let human_email_domain = optional(map, "BQ_HUMAN_EMAIL_DOMAIN").map(|value| {
            value
                .trim()
                .trim_start_matches('@')
                .to_lowercase()
                .to_string()
        });

        let api_base_url = optional(map, "BIGQUERY_API_BASE_URL")
            .unwrap_or_else(|| DEFAULT_API_BASE_URL.to_string())
            .trim_end_matches('/')
            .to_string();

        Ok(Self {
            project_id,
            region,
            cache_ttl_datasets,
            cache_ttl_dataset_details,
            cache_ttl_tables,
            cache_ttl_columns,
            cache_ttl_query_history,
            max_concurrency,
            query_timeout,
            human_email_domain,
            api_base_url,
        })
    }
}

fn required(map: &HashMap<String, String>, key: &str) -> Result<String> {
    optional(map, key).ok_or_else(|| {
        AppError::InvalidArgument(format!("Missing required environment variable: {key}"))
    })
}

fn optional(map: &HashMap<String, String>, key: &str) -> Option<String> {
    map.get(key)
        .cloned()
        .filter(|value| !value.trim().is_empty())
}

fn parse_u64(map: &HashMap<String, String>, key: &str, default_value: u64) -> Result<u64> {
    match optional(map, key) {
        Some(value) => value.parse::<u64>().map_err(|_| {
            AppError::InvalidArgument(format!("{key} must be an unsigned integer, got '{value}'"))
        }),
        None => Ok(default_value),
    }
}

fn parse_usize(map: &HashMap<String, String>, key: &str, default_value: usize) -> Result<usize> {
    match optional(map, key) {
        Some(value) => value.parse::<usize>().map_err(|_| {
            AppError::InvalidArgument(format!("{key} must be an unsigned integer, got '{value}'"))
        }),
        None => Ok(default_value),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_config_defaults() {
        let mut env = HashMap::new();
        env.insert("BIGQUERY_PROJECT_ID".to_string(), "proj".to_string());

        let config = Config::from_map(&env).expect("config should parse");

        assert_eq!(config.project_id, "proj");
        assert_eq!(config.region, "europe-west2");
        assert_eq!(config.cache_ttl_datasets.as_secs(), 60);
        assert_eq!(config.max_concurrency, 16);
    }

    #[test]
    fn test_missing_project_id() {
        let env = HashMap::new();
        let err = Config::from_map(&env).expect_err("should fail");
        assert!(err.to_string().contains("BIGQUERY_PROJECT_ID"));
    }

    #[test]
    fn test_domain_normalization() {
        let mut env = HashMap::new();
        env.insert("BIGQUERY_PROJECT_ID".to_string(), "proj".to_string());
        env.insert(
            "BQ_HUMAN_EMAIL_DOMAIN".to_string(),
            "@Example.COM".to_string(),
        );

        let config = Config::from_map(&env).expect("config should parse");
        assert_eq!(config.human_email_domain.as_deref(), Some("example.com"));
    }
}
