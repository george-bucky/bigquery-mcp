use std::sync::Arc;

use bigquery_mcp_rs::bigquery::client::BigQueryClient;
use bigquery_mcp_rs::config::Config;
use bigquery_mcp_rs::mcp::{McpServer, init_logging};
use bigquery_mcp_rs::service::Service;

#[tokio::main]
async fn main() {
    init_logging();

    let config = match Config::from_env() {
        Ok(config) => config,
        Err(err) => {
            eprintln!("failed to load config: {err}");
            std::process::exit(1);
        }
    };

    let backend = match BigQueryClient::new(config.clone()).await {
        Ok(client) => Arc::new(client),
        Err(err) => {
            eprintln!("failed to initialize BigQuery client: {err}");
            std::process::exit(1);
        }
    };

    let service = Arc::new(Service::new(&config, backend));
    let server = McpServer::new(service);

    if let Err(err) = server.run().await {
        eprintln!("MCP server failed: {err}");
        std::process::exit(1);
    }
}
