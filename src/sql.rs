use crate::error::{AppError, Result};

const DATASET_DESCRIPTIONS_SQL: &str = include_str!("sql_rs/datasets_descriptions.sql");
const TABLES_SQL: &str = include_str!("sql_rs/tables.sql");
const COLUMNS_SQL: &str = include_str!("sql_rs/columns.sql");
const JOBS_RECENT_SQL: &str = include_str!("sql_rs/jobs_recent.sql");
const JOBS_STABLE_SAMPLE_SQL: &str = include_str!("sql_rs/jobs_stable_sample.sql");

pub fn datasets_descriptions_sql(region: &str) -> String {
    DATASET_DESCRIPTIONS_SQL.replace("{region}", region)
}

pub fn tables_sql(project: &str, dataset: &str, region: &str) -> String {
    TABLES_SQL
        .replace("{project}", project)
        .replace("{dataset}", dataset)
        .replace("{region}", region)
}

pub fn columns_sql(project: &str, dataset: &str) -> String {
    COLUMNS_SQL
        .replace("{project}", project)
        .replace("{dataset}", dataset)
}

pub fn jobs_sql(region: &str, sample_mode: &str) -> Result<String> {
    let template = match sample_mode {
        "recent" => JOBS_RECENT_SQL,
        "stable_sample" => JOBS_STABLE_SAMPLE_SQL,
        invalid => {
            return Err(AppError::InvalidArgument(format!(
                "invalid sample_mode '{invalid}', expected 'recent' or 'stable_sample'"
            )));
        }
    };
    Ok(template.replace("{region}", region))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_jobs_sql_has_no_rand() {
        let sql = jobs_sql("europe-west2", "recent").expect("sql should render");
        assert!(!sql.to_uppercase().contains("RAND()"));
        assert!(sql.contains("TIMESTAMP_SUB"));
    }

    #[test]
    fn test_jobs_sql_mode_validation() {
        let err = jobs_sql("europe-west2", "bad").expect_err("should fail");
        assert!(err.to_string().contains("sample_mode"));
    }

    #[test]
    fn test_region_template_is_dynamic() {
        let sql = datasets_descriptions_sql("us-east1");
        assert!(sql.contains("region-us-east1"));
    }

    #[test]
    fn test_tables_sql_includes_region_storage_view() {
        let sql = tables_sql("my-project", "my_dataset", "us");
        assert!(sql.contains("region-us"));
        assert!(sql.contains("TABLE_STORAGE"));
    }
}
