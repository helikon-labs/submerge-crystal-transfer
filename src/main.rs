use clap::Parser;
use std::time::Duration;
use log::LevelFilter;
use crate::persistence::PostgreSQLStorage;

mod logging;
mod persistence;
mod types;

#[derive(Parser, Clone, Debug)]
pub struct Args {
    #[arg(long, default_value = "localhost")]
    pub src_host: String,
    #[arg(long, default_value = "5433")]
    pub src_port: u16,
    #[arg(long, default_value = "submerge_crystal_polkadot")]
    pub src_db: String,
    #[arg(long, default_value = "submerge")]
    pub src_user: String,
    #[arg(long, default_value = "submerge")]
    pub src_password: String,
    #[arg(long, default_value = "localhost")]
    pub dst_host: String,
    #[arg(long, default_value = "5432")]
    pub dst_port: u16,
    #[arg(long, default_value = "submerge_crystal")]
    pub dst_db: String,
    #[arg(long, default_value = "submerge")]
    pub dst_user: String,
    #[arg(long, default_value = "submerge")]
    pub dst_password: String,
    #[arg(long, default_value = "100")]
    pub chunk_size: u64,
}

const CONN_TIMEOUT_SECS: u64 = 10;
const POOL_MAX_CONNS: u32 = 10;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    logging::init_logging(LevelFilter::Debug, LevelFilter::Warn);
    let src = PostgreSQLStorage::new(
        args.src_host.as_str(),
        args.src_port,
        args.src_user.as_str(),
        args.src_password.as_str(),
        args.src_db.as_str(),
        CONN_TIMEOUT_SECS,
        POOL_MAX_CONNS,
    ).await?;
    /*
    let _dst = PostgreSQLStorage::new(
        args.dst_host.as_str(),
        args.dst_port,
        args.dst_user.as_str(),
        args.dst_password.as_str(),
        args.dst_db.as_str(),
        CONN_TIMEOUT_SECS,
        POOL_MAX_CONNS,
    ).await?;
     */
    let start_block_number = 0; // dst.get_next_block_number(0, u64::MAX).await?;
    log::info!("Start block number: {start_block_number}");
    let target_block_number = src.get_max_block_number().await?;
    log::info!("Target block number: {target_block_number}");
    let mut block_number = start_block_number;
    while block_number < target_block_number {
        log::info!("Get blocks {block_number}..={}", block_number + args.chunk_size);
        let traces = src.get_block_traces_by_number_range(
            block_number,
            block_number + args.chunk_size - 1,
        ).await?;
        log::info!("Got {} blocks.", traces.len());
        block_number += args.chunk_size;
    }
    Ok(())
}
