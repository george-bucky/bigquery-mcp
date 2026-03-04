use regex::Regex;

use crate::error::{AppError, Result};

fn identifier_regex() -> Regex {
    Regex::new(r"^[A-Za-z0-9_]+$").expect("valid regex")
}

fn region_regex() -> Regex {
    Regex::new(r"(^[a-z]+-[a-z0-9]+[0-9]$)|^(us|eu)$").expect("valid regex")
}

pub fn validate_identifier(label: &str, value: &str) -> Result<()> {
    if identifier_regex().is_match(value) {
        Ok(())
    } else {
        Err(AppError::InvalidArgument(format!(
            "{label} contains invalid characters: '{value}'"
        )))
    }
}

pub fn validate_region(value: &str) -> Result<()> {
    if region_regex().is_match(value) {
        Ok(())
    } else {
        Err(AppError::InvalidArgument(format!(
            "region is invalid: '{value}'"
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_identifier_validation() {
        assert!(validate_identifier("dataset", "my_dataset_1").is_ok());
        assert!(validate_identifier("dataset", "bad-name").is_err());
    }

    #[test]
    fn test_region_validation() {
        assert!(validate_region("europe-west2").is_ok());
        assert!(validate_region("us").is_ok());
        assert!(validate_region("europe_west2").is_err());
    }
}
