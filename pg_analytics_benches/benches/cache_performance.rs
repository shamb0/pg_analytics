use anyhow::{anyhow, Result};
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use datafusion::dataframe::DataFrame;
use datafusion::prelude::*;
use sqlx::PgConnection;
use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::runtime::Runtime;

pub mod fixtures {
    include!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../tests/fixtures/mod.rs"
    ));
}
use fixtures::*;

use crate::tables::auto_sales::AutoSalesSimulator;
use crate::tables::auto_sales::AutoSalesTestRunner;

pub const TOTAL_RECORDS: usize = 1_000_000;
pub const BATCH_SIZE: usize = 1_000;

struct BenchResource {
    df: Arc<DataFrame>,
    pg_conn: Arc<Mutex<PgConnection>>,
    s3_storage: Arc<S3>,
    pg_db: Arc<db::Db>,
    runtime: Runtime,
}

impl BenchResource {
    fn new() -> Result<Self> {
        let runtime = Runtime::new().expect("Failed to create Tokio runtime");

        let (df, s3_storage, pg_db, pg_conn) =
            runtime.block_on(async { Self::setup_benchmark().await })?;

        Ok(Self {
            df: Arc::new(df),
            pg_conn: Arc::new(Mutex::new(pg_conn)),
            s3_storage: Arc::new(s3_storage),
            pg_db: Arc::new(pg_db),
            runtime,
        })
    }

    async fn setup_benchmark() -> Result<(DataFrame, S3, db::Db, PgConnection)> {
        // Initialize database
        let db = db::Db::new().await;

        let mut pg_conn: PgConnection = db.connection().await;

        sqlx::query("CREATE EXTENSION IF NOT EXISTS pg_analytics;")
            .execute(&mut pg_conn)
            .await?;

        // Set up S3
        let s3_storage = S3::new().await;
        let s3_bucket = "demo-mlp-auto-sales";
        s3_storage.create_bucket(s3_bucket).await?;

        // Generate and load data
        let parquet_path = Self::parquet_path();
        tracing::warn!("parquet_path :: {:#?}", parquet_path);
        if !parquet_path.exists() {
            AutoSalesSimulator::save_to_parquet_in_batches(
                TOTAL_RECORDS,
                BATCH_SIZE,
                &parquet_path,
            )?;
        }

        // Create DataFrame from Parquet file
        let ctx = SessionContext::new();
        let df = ctx
            .read_parquet(
                parquet_path.to_str().unwrap(),
                ParquetReadOptions::default(),
            )
            .await?;

        // Partition data and upload to S3
        AutoSalesTestRunner::create_partition_and_upload_to_s3(&s3_storage, s3_bucket, &df).await?;

        Ok((df, s3_storage, db, pg_conn))
    }

    fn parquet_path() -> PathBuf {
        // Use the environment variable to detect the `target` path
        let target_dir = env::var("CARGO_TARGET_DIR").unwrap_or_else(|_| "target".to_string());
        let parquet_path = Path::new(&target_dir).join("tmp_dataset/ds_auto_sales.parquet");

        // Check if the file exists; if not, create the necessary directories
        if !parquet_path.exists() {
            if let Some(parent_dir) = parquet_path.parent() {
                fs::create_dir_all(parent_dir).expect("Failed to create directories");
            }
        }

        parquet_path
    }

    async fn setup_tables(
        &self,
        s3_bucket: &str,
        foreign_table_id: &str,
        with_disk_cache: bool,
        with_mem_cache: bool,
    ) -> Result<()> {
        // Clone Arc to avoid holding the lock across await points
        let pg_conn = Arc::clone(&self.pg_conn);
        let s3_storage = Arc::clone(&self.s3_storage);

        // Use a separate block to ensure the lock is released as soon as possible
        {
            let mut pg_conn = pg_conn
                .lock()
                .map_err(|e| anyhow::anyhow!("Failed to acquire database lock: {}", e))?;

            AutoSalesTestRunner::setup_tables(
                &mut pg_conn,
                &s3_storage,
                s3_bucket,
                foreign_table_id,
                with_disk_cache,
            )
            .await?;

            let with_mem_cache_cfg = if with_mem_cache { "true" } else { "false" };
            let query = format!(
                "SELECT duckdb_execute($$SET enable_object_cache={}$$)",
                with_mem_cache_cfg
            );
            sqlx::query(&query).execute(&mut *pg_conn).await?;
        }

        Ok(())
    }

    async fn bench_total_sales(&self, foreign_table_id: &str) -> Result<()> {
        let pg_conn = Arc::clone(&self.pg_conn);

        let mut conn = pg_conn
            .lock()
            .map_err(|e| anyhow::anyhow!("Failed to acquire database lock: {}", e))?;

        let _ =
            AutoSalesTestRunner::assert_total_sales(&mut conn, &self.df, foreign_table_id, true)
                .await;

        Ok(())
    }

    fn cleanup(&mut self) -> Result<()> {
        self.runtime.block_on(async {
            // S3 cleanup
            Arc::get_mut(&mut self.s3_storage)
                .ok_or_else(|| anyhow!("Failed to get mutable reference to s3_storage"))?
                .cleanup()
                .await;

            // PgDb cleanup
            Arc::get_mut(&mut self.pg_db)
                .ok_or_else(|| anyhow!("Failed to get mutable reference to pg_db"))?
                .cleanup()
                .await;

            Ok(())
        })
    }
}

impl Drop for BenchResource {
    fn drop(&mut self) {
        if let Err(e) = self.cleanup() {
            tracing::error!("BenchResource cleanup failed: {}", e);
        }
    }
}

pub fn full_cache_bench(c: &mut Criterion) {
    print_utils::init_tracer();
    tracing::info!("Starting full cache benchmark");

    let bench_resource = match BenchResource::new() {
        Ok(resource) => resource,
        Err(e) => {
            tracing::error!("Failed to initialize BenchResource: {}", e);
            return;
        }
    };

    let s3_bucket = "demo-mlp-auto-sales";
    let foreign_table_id = "auto_sales_full_cache";

    let mut group = c.benchmark_group("Parquet Full Cache Benchmarks");
    group.sample_size(10); // Adjust sample size if necessary

    // Setup tables for the benchmark
    bench_resource.runtime.block_on(async {
        if let Err(e) = bench_resource
            .setup_tables(s3_bucket, foreign_table_id, true, true)
            .await
        {
            tracing::error!("Table setup failed: {}", e);
        }
    });

    // Run the benchmark with full cache
    group
        .sample_size(10)
        .measurement_time(Duration::from_secs(3))
        .warm_up_time(Duration::from_secs(1))
        .throughput(criterion::Throughput::Elements(TOTAL_RECORDS as u64))
        .bench_function(BenchmarkId::new("Auto Sales", "Full Cache"), |b| {
            b.to_async(&bench_resource.runtime).iter(|| async {
                bench_resource
                    .bench_total_sales(foreign_table_id)
                    .await
                    .unwrap();
            });
        });

    tracing::info!("Full cache benchmark completed");
    group.finish();
}

pub fn disk_cache_bench(c: &mut Criterion) {
    print_utils::init_tracer();
    tracing::info!("Starting disk cache benchmark");

    let bench_resource = match BenchResource::new() {
        Ok(resource) => resource,
        Err(e) => {
            tracing::error!("Failed to initialize BenchResource: {}", e);
            return;
        }
    };

    let s3_bucket = "demo-mlp-auto-sales";
    let foreign_table_id = "auto_sales_disk_cache";

    let mut group = c.benchmark_group("Parquet Disk Cache Benchmarks");
    group.sample_size(10); // Adjust sample size if necessary

    // Setup tables for the benchmark
    bench_resource.runtime.block_on(async {
        if let Err(e) = bench_resource
            .setup_tables(s3_bucket, foreign_table_id, true, false)
            .await
        {
            tracing::error!("Table setup failed: {}", e);
        }
    });

    // Run the benchmark with disk cache
    group
        .sample_size(10)
        .measurement_time(Duration::from_secs(3))
        .warm_up_time(Duration::from_secs(1))
        .throughput(criterion::Throughput::Elements(TOTAL_RECORDS as u64))
        .bench_function(BenchmarkId::new("Auto Sales", "Disk Cache"), |b| {
            b.to_async(&bench_resource.runtime).iter(|| async {
                bench_resource
                    .bench_total_sales(foreign_table_id)
                    .await
                    .unwrap();
            });
        });

    tracing::info!("Disk cache benchmark completed");
    group.finish();
}

pub fn mem_cache_bench(c: &mut Criterion) {
    print_utils::init_tracer();
    tracing::info!("Starting Mem cache benchmark");

    let bench_resource = match BenchResource::new() {
        Ok(resource) => resource,
        Err(e) => {
            tracing::error!("Failed to initialize BenchResource: {}", e);
            return;
        }
    };

    let s3_bucket = "demo-mlp-auto-sales";
    let foreign_table_id = "auto_sales_mem_cache";

    let mut group = c.benchmark_group("Parquet Mem Cache Benchmarks");
    group.sample_size(10); // Adjust sample size if necessary

    // Setup tables for the benchmark
    bench_resource.runtime.block_on(async {
        if let Err(e) = bench_resource
            .setup_tables(s3_bucket, foreign_table_id, false, true)
            .await
        {
            tracing::error!("Table setup failed: {}", e);
        }
    });

    // Run the benchmark with mem cache
    group
        .sample_size(10)
        .measurement_time(Duration::from_secs(3))
        .warm_up_time(Duration::from_secs(1))
        .throughput(criterion::Throughput::Elements(TOTAL_RECORDS as u64))
        .bench_function(BenchmarkId::new("Auto Sales", "Mem Cache"), |b| {
            b.to_async(&bench_resource.runtime).iter(|| async {
                bench_resource
                    .bench_total_sales(foreign_table_id)
                    .await
                    .unwrap();
            });
        });

    tracing::info!("Mem cache benchmark completed");
    group.finish();
}

pub fn no_cache_bench(c: &mut Criterion) {
    print_utils::init_tracer();
    tracing::info!("Starting no cache benchmark");

    let bench_resource = match BenchResource::new() {
        Ok(resource) => resource,
        Err(e) => {
            tracing::error!("Failed to initialize BenchResource: {}", e);
            return;
        }
    };

    let s3_bucket = "demo-mlp-auto-sales";
    let foreign_table_id = "auto_sales_no_cache";

    let mut group = c.benchmark_group("Parquet No Cache Benchmarks");
    group.sample_size(10); // Adjust sample size if necessary

    // Setup tables for the benchmark
    bench_resource.runtime.block_on(async {
        if let Err(e) = bench_resource
            .setup_tables(s3_bucket, foreign_table_id, false, false)
            .await
        {
            tracing::error!("Table setup failed: {}", e);
        }
    });

    // Run the benchmark with no cache
    group
        .sample_size(10)
        .measurement_time(Duration::from_secs(3))
        .warm_up_time(Duration::from_secs(1))
        .throughput(criterion::Throughput::Elements(TOTAL_RECORDS as u64))
        .bench_function(BenchmarkId::new("Auto Sales", "No Cache"), |b| {
            b.to_async(&bench_resource.runtime).iter(|| async {
                bench_resource
                    .bench_total_sales(foreign_table_id)
                    .await
                    .unwrap();
            });
        });

    tracing::info!("No cache benchmark completed");
    group.finish();
}

criterion_group!(
    name = parquet_ft_bench;
    config = Criterion::default().measurement_time(std::time::Duration::from_secs(30));
    targets = no_cache_bench, disk_cache_bench, mem_cache_bench, full_cache_bench
);

criterion_main!(parquet_ft_bench);
