use crate::types::{BlockTrace, BlockTraces, StorageMethod};
use sqlx::{Pool, Postgres};
use std::str::FromStr;
use std::time::Duration;

async fn new_postgres_connection_pool(
    host: &str,
    port: u16,
    username: &str,
    password: &str,
    database_name: &str,
    connection_timeout_secs: u64,
    pool_max_connections: u32,
) -> anyhow::Result<Pool<Postgres>> {
    log::info!("⚙️ Establishing PostgreSQL connection pool.");
    let connection_str =
        format!("postgres://{username}:{password}@{host}:{port}/{database_name}?sslmode=disable");
    let connection_pool = sqlx::postgres::PgPoolOptions::new()
        .acquire_timeout(Duration::from_secs(connection_timeout_secs))
        .max_connections(pool_max_connections)
        .connect(&connection_str)
        .await?;
    log::info!("✅ PostgreSQL connection pool established.");
    Ok(connection_pool)
}

pub(crate) struct PostgreSQLStorage {
    connection_pool: Pool<Postgres>,
}

impl PostgreSQLStorage {
    pub async fn new(
        host: &str,
        port: u16,
        username: &str,
        password: &str,
        database_name: &str,
        connection_timeout_secs: u64,
        pool_max_connections: u32,
    ) -> anyhow::Result<PostgreSQLStorage> {
        Ok(PostgreSQLStorage {
            connection_pool: new_postgres_connection_pool(
                host,
                port,
                username,
                password,
                database_name,
                connection_timeout_secs,
                pool_max_connections,
            )
            .await?,
        })
    }
}

impl PostgreSQLStorage {
    pub(crate) async fn block_trace_exists(&self, block_hash: &str) -> anyhow::Result<bool> {
        let block_hash = hex::decode(block_hash)?;
        let record_count: (i64,) = sqlx::query_as(
            r#"
            SELECT COUNT(DISTINCT trace_index)
            FROM trace
            WHERE block_hash = $1
            "#,
        )
        .bind(block_hash)
        .fetch_one(&self.connection_pool)
        .await?;
        Ok(record_count.0 > 0)
    }

    pub(crate) async fn get_next_block_number(&self, min: u64, max: u64) -> anyhow::Result<u64> {
        let row: (Option<i64>,) = sqlx::query_as(
            "SELECT MAX(block_number) FROM trace WHERE block_number >= $1 AND block_number <= $2",
        )
        .bind(min as i64)
        .bind(max as i64)
        .fetch_one(&self.connection_pool)
        .await?;
        Ok(if let Some(min_in_range) = row.0 {
            min_in_range as u64 + 1
        } else {
            min
        })
    }

    pub(crate) async fn get_max_block_number(&self) -> anyhow::Result<u64> {
        let row: (i64,) = sqlx::query_as(
            "SELECT MAX(block_number) FROM trace",
        )
            .fetch_one(&self.connection_pool)
            .await?;
        Ok(row.0 as u64)
    }

    pub(crate) async fn get_block_traces_by_number_range(
        &self,
        start_block_number: u64,
        end_block_number: u64,
    ) -> anyhow::Result<Vec<BlockTraces>> {
        let block_hash_rows: Vec<(Vec<u8>,)> =
            sqlx::query_as("SELECT DISTINCT block_hash FROM trace WHERE block_number >= $1 AND block_number <= $2")
                .bind(start_block_number as i64)
                .bind(end_block_number as i64)
                .fetch_all(&self.connection_pool)
                .await?;
        let mut result = vec![];
        for block_hash_row in block_hash_rows.iter() {
            if let Some(block_traces) = self.get_block_traces_by_hash(&block_hash_row.0).await? {
                result.push(block_traces);
            }
        }
        Ok(result)
    }

    pub(crate) async fn get_block_traces_by_number(
        &self,
        block_number: u64,
    ) -> anyhow::Result<Vec<BlockTraces>> {
        let block_hash_rows: Vec<(Vec<u8>,)> =
            sqlx::query_as("SELECT DISTINCT block_hash FROM trace WHERE block_number = $1")
                .bind(block_number as i64)
                .fetch_all(&self.connection_pool)
                .await?;
        let mut result = vec![];
        for block_hash_row in block_hash_rows.iter() {
            if let Some(block_traces) = self.get_block_traces_by_hash(&block_hash_row.0).await? {
                result.push(block_traces);
            }
        }
        Ok(result)
    }

    pub(crate) async fn get_block_traces_by_hash(
        &self,
        block_hash: &Vec<u8>,
    ) -> anyhow::Result<Option<BlockTraces>> {
        #[allow(clippy::type_complexity)]
        let rows: Vec<(Vec<u8>, i64, i32, bool, i32, String, String, String, String, Option<String>)> = sqlx::query_as("SELECT block_parent_hash, block_number, runtime_version, is_finalized, trace_index, key, value, ext_id, method, parent_id FROM trace WHERE block_hash = $1 ORDER BY trace_index ASC")
            .bind(block_hash)
            .fetch_all(&self.connection_pool)
            .await?;

        if let Some(first_row) = rows.first() {
            let block_hash_hex = format!("0x{}", hex::encode(block_hash));
            let block_parent_hash_hex = format!("0x{}", hex::encode(&first_row.0));
            let mut block_traces = BlockTraces {
                block_hash: block_hash_hex,
                block_parent_hash: block_parent_hash_hex,
                block_number: first_row.1 as u64,
                runtime_version: first_row.2 as u32,
                is_finalized: first_row.3,
                traces: vec![],
            };
            for row in rows.iter() {
                block_traces.traces.push(BlockTrace {
                    index: row.4 as u32,
                    key: row.5.clone(),
                    value: row.6.clone(),
                    ext_id: row.7.clone(),
                    method: StorageMethod::from_str(&row.8)?,
                    parent_id: row.9.clone(),
                })
            }
            Ok(Some(block_traces))
        } else {
            Ok(None)
        }
    }
}
