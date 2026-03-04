use std::sync::Arc;

use serde::Deserialize;
use serde::Serialize;
use serde_json::{Value, json};
use tokio::io::{self, AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader, Stdout};
use tokio::sync::Mutex;
use tracing::{error, info};

use crate::error::AppError;
use crate::service::Service;
use crate::types::{
    GetAllDatasetDescriptionsInput, GetColumnsInput, GetDatasetDescriptionInput, GetDatasetsInput,
    GetQueryHistoryInput, GetTablesInput,
};

#[derive(Debug, Deserialize)]
struct RpcRequest {
    #[serde(default)]
    id: Option<Value>,
    method: String,
    #[serde(default)]
    params: Option<Value>,
}

#[derive(Debug, Serialize)]
struct RpcError {
    code: i64,
    message: String,
    data: Value,
}

#[derive(Clone)]
pub struct McpServer {
    service: Arc<Service>,
}

impl McpServer {
    pub fn new(service: Arc<Service>) -> Self {
        Self { service }
    }

    pub async fn run(&self) -> io::Result<()> {
        let stdin = io::stdin();
        let stdout = io::stdout();

        let mut reader = BufReader::new(stdin);
        let writer = Arc::new(Mutex::new(stdout));

        loop {
            let Some(message) = read_message(&mut reader).await? else {
                break;
            };

            let request: RpcRequest = match serde_json::from_slice(&message) {
                Ok(request) => request,
                Err(err) => {
                    let error = json!({
                        "jsonrpc": "2.0",
                        "error": {
                            "code": -32700,
                            "message": format!("parse error: {err}")
                        },
                        "id": Value::Null
                    });
                    write_message(&writer, &error).await?;
                    continue;
                }
            };

            match self.handle_request(request).await {
                Ok(Some(response)) => {
                    write_message(&writer, &response).await?;
                }
                Ok(None) => {}
                Err(err) => {
                    error!(error = %err, "failed to handle request");
                }
            }
        }

        Ok(())
    }

    async fn handle_request(&self, request: RpcRequest) -> io::Result<Option<Value>> {
        let id = request.id.clone();

        if id.is_none() {
            if request.method == "notifications/initialized" {
                return Ok(None);
            }
            return Ok(None);
        }

        let result = match request.method.as_str() {
            "initialize" => Ok(json!({
                "protocolVersion": "2025-03-26",
                "capabilities": {
                    "tools": {
                        "listChanged": false
                    }
                },
                "serverInfo": {
                    "name": "bigquery-mcp-rs",
                    "version": env!("CARGO_PKG_VERSION")
                }
            })),
            "tools/list" => Ok(json!({
                "tools": tool_definitions()
            })),
            "tools/call" => self.call_tool(request.params).await,
            other => Err(AppError::NotFound(format!("unknown method: {other}"))),
        };

        let response = match result {
            Ok(result) => json!({
                "jsonrpc": "2.0",
                "id": id,
                "result": result
            }),
            Err(err) => {
                let rpc_error = RpcError {
                    code: err.jsonrpc_code(),
                    message: err.to_string(),
                    data: json!({
                        "type": err.kind(),
                        "retryable": err.retryable(),
                        "request_id": id,
                    }),
                };
                json!({
                    "jsonrpc": "2.0",
                    "id": id,
                    "error": rpc_error,
                })
            }
        };

        Ok(Some(response))
    }

    async fn call_tool(&self, params: Option<Value>) -> Result<Value, AppError> {
        let params = params.unwrap_or_else(|| json!({}));
        let name = params
            .get("name")
            .and_then(Value::as_str)
            .ok_or_else(|| AppError::InvalidArgument("tools/call requires 'name'".to_string()))?;
        let arguments = params
            .get("arguments")
            .cloned()
            .unwrap_or_else(|| json!({}));

        let structured = match name {
            "get_datasets" => {
                let input: GetDatasetsInput = from_arguments(arguments)?;
                let output = self.service.get_datasets(input).await?;
                serde_json::to_value(output).map_err(map_serde_err)?
            }
            "get_all_dataset_descriptions" => {
                let input: GetAllDatasetDescriptionsInput = from_arguments(arguments)?;
                let output = self.service.get_all_dataset_descriptions(input).await?;
                serde_json::to_value(output).map_err(map_serde_err)?
            }
            "get_dataset_description" => {
                let input: GetDatasetDescriptionInput = from_arguments(arguments)?;
                let output = self.service.get_dataset_description(input).await?;
                serde_json::to_value(output).map_err(map_serde_err)?
            }
            "get_tables" => {
                let input: GetTablesInput = from_arguments(arguments)?;
                let output = self.service.get_tables(input).await?;
                serde_json::to_value(output).map_err(map_serde_err)?
            }
            "get_columns" => {
                let input: GetColumnsInput = from_arguments(arguments)?;
                let output = self.service.get_columns(input).await?;
                serde_json::to_value(output).map_err(map_serde_err)?
            }
            "get_query_history" => {
                let input: GetQueryHistoryInput = from_arguments(arguments)?;
                let output = self.service.get_query_history(input).await?;
                serde_json::to_value(output).map_err(map_serde_err)?
            }
            other => return Err(AppError::NotFound(format!("unknown tool '{other}'"))),
        };

        Ok(json!({
            "isError": false,
            "content": [
                {
                    "type": "text",
                    "text": serde_json::to_string_pretty(&structured).map_err(map_serde_err)?
                }
            ],
            "structuredContent": structured
        }))
    }
}

fn from_arguments<T: for<'de> Deserialize<'de>>(value: Value) -> Result<T, AppError> {
    serde_json::from_value(value).map_err(|err| {
        AppError::InvalidArgument(format!("invalid tool arguments for requested tool: {err}"))
    })
}

fn map_serde_err(err: serde_json::Error) -> AppError {
    AppError::Serialization(err.to_string())
}

async fn read_message(reader: &mut BufReader<io::Stdin>) -> io::Result<Option<Vec<u8>>> {
    let mut content_length: Option<usize> = None;

    loop {
        let mut line = String::new();
        let bytes = reader.read_line(&mut line).await?;
        if bytes == 0 {
            return Ok(None);
        }

        let line = line.trim_end_matches(['\r', '\n']);

        if line.is_empty() {
            break;
        }

        if let Some(value) = line.strip_prefix("Content-Length:") {
            let parsed = value.trim().parse::<usize>().map_err(|err| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("invalid Content-Length: {err}"),
                )
            })?;
            content_length = Some(parsed);
        }
    }

    let length = content_length.ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            "missing Content-Length header in incoming message",
        )
    })?;

    let mut body = vec![0_u8; length];
    reader.read_exact(&mut body).await?;
    Ok(Some(body))
}

async fn write_message(writer: &Arc<Mutex<Stdout>>, value: &Value) -> io::Result<()> {
    let payload = serde_json::to_vec(value)
        .map_err(|err| io::Error::new(io::ErrorKind::InvalidData, err.to_string()))?;
    let header = format!("Content-Length: {}\r\n\r\n", payload.len());

    let mut writer = writer.lock().await;
    writer.write_all(header.as_bytes()).await?;
    writer.write_all(&payload).await?;
    writer.flush().await?;
    Ok(())
}

fn tool_definitions() -> Vec<Value> {
    vec![
        json!({
            "name": "get_datasets",
            "description": "Return dataset IDs from the configured BigQuery project.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "limit": {"type": "integer", "minimum": 1, "maximum": 1000, "default": 200},
                    "cursor": {"type": ["string", "null"]}
                }
            }
        }),
        json!({
            "name": "get_all_dataset_descriptions",
            "description": "Return all dataset descriptions in the configured region.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "limit": {"type": "integer", "minimum": 1, "maximum": 1000, "default": 500},
                    "cursor": {"type": ["string", "null"]}
                }
            }
        }),
        json!({
            "name": "get_dataset_description",
            "description": "Return metadata for one dataset.",
            "inputSchema": {
                "type": "object",
                "required": ["dataset_id"],
                "properties": {
                    "dataset_id": {"type": "string"}
                }
            }
        }),
        json!({
            "name": "get_tables",
            "description": "Return relation metadata for one dataset via INFORMATION_SCHEMA.",
            "inputSchema": {
                "type": "object",
                "required": ["dataset"],
                "properties": {
                    "dataset": {"type": "string"},
                    "limit": {"type": "integer", "minimum": 1, "maximum": 1000, "default": 500},
                    "cursor": {"type": ["string", "null"]},
                    "include_without_description": {"type": "boolean", "default": false}
                }
            }
        }),
        json!({
            "name": "get_columns",
            "description": "Return column and nested field descriptions for one table.",
            "inputSchema": {
                "type": "object",
                "required": ["dataset", "table"],
                "properties": {
                    "dataset": {"type": "string"},
                    "table": {"type": "string"},
                    "include_undocumented": {"type": "boolean", "default": false}
                }
            }
        }),
        json!({
            "name": "get_query_history",
            "description": "Return query history for jobs that referenced a dataset.table.",
            "inputSchema": {
                "type": "object",
                "required": ["dataset", "table"],
                "properties": {
                    "dataset": {"type": "string"},
                    "table": {"type": "string"},
                    "limit": {"type": "integer", "minimum": 1, "maximum": 1000, "default": 10},
                    "lookback_days": {"type": "integer", "minimum": 1, "default": 7},
                    "sample_mode": {"type": "string", "enum": ["recent", "stable_sample"], "default": "recent"}
                }
            }
        }),
    ]
}

pub fn init_logging() {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_target(false)
        .compact()
        .init();

    info!("logging initialized");
}
