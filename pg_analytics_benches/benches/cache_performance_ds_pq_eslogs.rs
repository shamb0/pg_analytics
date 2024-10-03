use anyhow::{Context, Result};
use cargo_metadata::MetadataCommand;
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use datafusion::dataframe::DataFrame;
use datafusion::prelude::*;
use rand::Rng;
use sqlx::PgConnection;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::runtime::Runtime;

use camino::Utf8PathBuf;
use pga_fixtures::tables::eslogs::{EsLogParquetForeignTableManager, EsLogParquetManager, EsLogBenchManager};
use pga_fixtures::*;

const TOTAL_RECORDS: usize = 10_000;
const BATCH_SIZE: usize = 512;

// Constants for benchmark configuration
const SAMPLE_SIZE: usize = 10;
const MEASUREMENT_TIME_SECS: u64 = 30;
const WARM_UP_TIME_SECS: u64 = 2;

const S3_BUCKET_ID: &str = "demo-mlp-eslogs";
const S3_PREFIX: &str = "eslogs_dataset";

struct BenchResource {
    df: Arc<DataFrame>,
    pg_conn: Arc<Mutex<PgConnection>>,
    s3_storage: Arc<S3>,
    runtime: Runtime,
}

impl BenchResource {
    fn new() -> Result<Self> {
        let runtime = Runtime::new().expect("Failed to create Tokio runtime");

        let (df, s3_storage, pg_conn) =
            runtime.block_on(async { Self::setup_benchmark().await })?;

        Ok(Self {
            df: Arc::new(df),
            pg_conn: Arc::new(Mutex::new(pg_conn)),
            s3_storage: Arc::new(s3_storage),
            runtime,
        })
    }

    async fn setup_benchmark() -> Result<(DataFrame, S3, PgConnection)> {
        // Initialize database
        let db = db::Db::new().await;

        let mut pg_conn: PgConnection = db.connection().await;

        sqlx::query("CREATE EXTENSION IF NOT EXISTS pg_analytics;")
            .execute(&mut pg_conn)
            .await?;

        // Generate and load data
        let parquet_dataset_path = Self::parquet_path();
        tracing::warn!("parquet_path :: {:#?}", parquet_dataset_path);

        // Check if the Parquet file already exists at the specified path.
        if !parquet_dataset_path.exists() || !Self::dataset_exists(&parquet_dataset_path) {
            let seed = Self::generate_seed();
            tracing::info!("Generating ESLogs dataset with seed: {}", seed);
            EsLogParquetManager::create_hive_partitioned_parquet(
                10000,
                seed,
                1000,
                &parquet_dataset_path,
            )
            .context("Failed to generate and save ESLogs dataset as local Parquet")?;
        } else {
            tracing::warn!(
                "Path Status :: exists: {}, contains files: {}",
                parquet_dataset_path.exists(),
                Self::dataset_exists(&parquet_dataset_path)
            );
        }

        // Verify that the Parquet files were created
        if !Self::dataset_exists(&parquet_dataset_path) {
            tracing::error!(
                "Parquet dataset was not created at {:?}",
                parquet_dataset_path
            );
            return Err(anyhow::anyhow!("Parquet dataset was not created"));
        }

        // List the contents of the dataset directory
        tracing::debug!("Listing contents of dataset directory:");
        for entry in fs::read_dir(&parquet_dataset_path)? {
            let entry = entry?;
            tracing::debug!("{:?}", entry.path());
        }

        // Create DataFrame from Parquet file
        let eslogs_df = EsLogParquetManager::read_parquet_dataset(&parquet_dataset_path)
            .await
            .context("Failed to read ESLogs dataset from local Parquet")?;

        // Set up S3
        let s3_storage = S3::new().await;

        EsLogParquetManager::upload_parquet_dataset_to_s3(
            &s3_storage,
            S3_BUCKET_ID,
            S3_PREFIX,
            parquet_dataset_path,
        )
        .await
        .context("Failed to upload Parquet dataset to S3")?;

        s3_storage
            .print_object_list(S3_BUCKET_ID, S3_PREFIX)
            .await
            .context("Failed to list objects in S3")?;

        Ok((eslogs_df, s3_storage, pg_conn))
    }

    fn parquet_path() -> PathBuf {
        let target_dir = MetadataCommand::new()
            .no_deps()
            .exec()
            .map(|metadata| metadata.workspace_root)
            .unwrap_or_else(|err| {
                tracing::warn!(
                    "Failed to get workspace root: {}. Using 'target' as fallback.",
                    err
                );
                Utf8PathBuf::from("target")
            });

        let parquet_path = target_dir
            .join("target")
            .join("tmp_dataset")
            .join("eslogs_dataset");

        // Check if the file exists; if not, create the necessary directories
        if !parquet_path.exists() {
            if let Some(parent_dir) = parquet_path.parent() {
                fs::create_dir_all(parent_dir)
                    .with_context(|| format!("Failed to create directory: {:#?}", parent_dir))
                    .unwrap_or_else(|err| {
                        tracing::error!("{}", err);
                        panic!("Critical error: {}", err);
                    });
            }
        }

        parquet_path.into()
    }

    /// Generates a random seed for reproducible data generation.
    fn generate_seed() -> u64 {
        let mut rng = rand::thread_rng();
        let seed: u32 = rng.gen();
        tracing::debug!("Generated random seed: {}", seed);
        u64::from(seed)
    }

    /// Checks if the given path is a directory and is empty.
    fn dataset_exists(base_path: &Path) -> bool {
        let exists = base_path.exists()
            && base_path
                .read_dir()
                .map(|mut i| i.next().is_some())
                .unwrap_or(false);
        tracing::debug!("Dataset exists at {:?}: {}", base_path, exists);
        exists
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

            EsLogParquetForeignTableManager::setup_tables(
                &mut pg_conn,
                &s3_storage,
                S3_BUCKET_ID,
                S3_PREFIX,
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
            EsLogBenchManager::bench_time_range_query(&mut conn, &self.df, foreign_table_id)
                .await;
                
        Ok(())
    }
}

pub fn eslog_pq_disk_cache_bench(c: &mut Criterion) {
    print_utils::init_tracer();
    tracing::info!("Starting ESLog PQ disk cache benchmark");

    let bench_resource = match BenchResource::new() {
        Ok(resource) => resource,
        Err(e) => {
            tracing::error!("Failed to initialize BenchResource: {}", e);
            return;
        }
    };

    let foreign_table_id = "eslog_pq_disk_cache";

    let mut group = c.benchmark_group("Eslog PQ Disk Cache Benchmarks");
    group.sample_size(10); // Adjust sample size if necessary

    // Setup tables for the benchmark
    bench_resource.runtime.block_on(async {
        if let Err(e) = bench_resource
            .setup_tables(S3_BUCKET_ID, foreign_table_id, true, false)
            .await
        {
            tracing::error!("Table setup failed: {}", e);
        }
    });

    // Run the benchmark with no cache
    group
        .sample_size(SAMPLE_SIZE)
        .measurement_time(Duration::from_secs(MEASUREMENT_TIME_SECS))
        .warm_up_time(Duration::from_secs(WARM_UP_TIME_SECS))
        .throughput(criterion::Throughput::Elements(TOTAL_RECORDS as u64))
        .bench_function(BenchmarkId::new("ESLog PQ", "Disk Cache"), |b| {
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


pub fn eslog_pq_mem_cache_bench(c: &mut Criterion) {
    print_utils::init_tracer();
    tracing::info!("Starting ESLog PQ mem cache benchmark");

    let bench_resource = match BenchResource::new() {
        Ok(resource) => resource,
        Err(e) => {
            tracing::error!("Failed to initialize BenchResource: {}", e);
            return;
        }
    };

    let foreign_table_id = "eslog_pq_mem_cache";

    let mut group = c.benchmark_group("Eslog PQ Mem Cache Benchmarks");
    group.sample_size(10); // Adjust sample size if necessary

    // Setup tables for the benchmark
    bench_resource.runtime.block_on(async {
        if let Err(e) = bench_resource
            .setup_tables(S3_BUCKET_ID, foreign_table_id, false, true)
            .await
        {
            tracing::error!("Table setup failed: {}", e);
        }
    });

    // Run the benchmark with no cache
    group
        .sample_size(SAMPLE_SIZE)
        .measurement_time(Duration::from_secs(MEASUREMENT_TIME_SECS))
        .warm_up_time(Duration::from_secs(WARM_UP_TIME_SECS))
        .throughput(criterion::Throughput::Elements(TOTAL_RECORDS as u64))
        .bench_function(BenchmarkId::new("ESLog PQ", "Mem Cache"), |b| {
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

pub fn eslog_pq_full_cache_bench(c: &mut Criterion) {
    print_utils::init_tracer();
    tracing::info!("Starting ESLog PQ Full cache benchmark");

    let bench_resource = match BenchResource::new() {
        Ok(resource) => resource,
        Err(e) => {
            tracing::error!("Failed to initialize BenchResource: {}", e);
            return;
        }
    };

    let foreign_table_id = "eslog_pq_full_cache";

    let mut group = c.benchmark_group("Eslog PQ Full Cache Benchmarks");
    group.sample_size(10); // Adjust sample size if necessary

    // Setup tables for the benchmark
    bench_resource.runtime.block_on(async {
        if let Err(e) = bench_resource
            .setup_tables(S3_BUCKET_ID, foreign_table_id, true, true)
            .await
        {
            tracing::error!("Table setup failed: {}", e);
        }
    });

    // Run the benchmark with no cache
    group
        .sample_size(SAMPLE_SIZE)
        .measurement_time(Duration::from_secs(MEASUREMENT_TIME_SECS))
        .warm_up_time(Duration::from_secs(WARM_UP_TIME_SECS))
        .throughput(criterion::Throughput::Elements(TOTAL_RECORDS as u64))
        .bench_function(BenchmarkId::new("ESLog PQ", "Full Cache"), |b| {
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

pub fn eslog_pq_no_cache_bench(c: &mut Criterion) {
    print_utils::init_tracer();
    tracing::info!("Starting ESLog PQ no cache benchmark");

    let bench_resource = match BenchResource::new() {
        Ok(resource) => resource,
        Err(e) => {
            tracing::error!("Failed to initialize BenchResource: {}", e);
            return;
        }
    };

    let foreign_table_id = "eslog_no_cache";

    let mut group = c.benchmark_group("Eslog PQ No Cache Benchmarks");
    group.sample_size(10); // Adjust sample size if necessary

    // Setup tables for the benchmark
    bench_resource.runtime.block_on(async {
        if let Err(e) = bench_resource
            .setup_tables(S3_BUCKET_ID, foreign_table_id, false, false)
            .await
        {
            tracing::error!("Table setup failed: {}", e);
        }
    });

    // Run the benchmark with no cache
    group
        .sample_size(SAMPLE_SIZE)
        .measurement_time(Duration::from_secs(MEASUREMENT_TIME_SECS))
        .warm_up_time(Duration::from_secs(WARM_UP_TIME_SECS))
        .throughput(criterion::Throughput::Elements(TOTAL_RECORDS as u64))
        .bench_function(BenchmarkId::new("ESLog PQ", "No Cache"), |b| {
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
    name = eslog_pq_ft_bench;
    config = Criterion::default().measurement_time(std::time::Duration::from_secs(240));
    targets = eslog_pq_disk_cache_bench, eslog_pq_mem_cache_bench, eslog_pq_full_cache_bench, eslog_pq_no_cache_bench
);

criterion_main!(eslog_pq_ft_bench);
