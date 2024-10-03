// Copyright (c) 2023-2024 Retake, Inc.
//
// This file is part of ParadeDB - Postgres for Search and Analytics
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program. If not, see <http://www.gnu.org/licenses/>.

use std::env;
use std::fs;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use rand::Rng;
use rstest::*;
use sqlx::PgConnection;

use datafusion::datasource::file_format::options::ParquetReadOptions;
use datafusion::prelude::SessionContext;
use pga_fixtures::tables::eslogs::{
    EsLogParquetForeignTableManager, EsLogParquetManager, EsLogTestManager,
};
use pga_fixtures::*;

/// Provides the path for the Parquet dataset used in benchmarking.
#[fixture]
fn parquet_dataset_path() -> PathBuf {
    let target_dir = env::var("CARGO_TARGET_DIR").unwrap_or_else(|_| "target".to_string());
    let dataset_path = Path::new(&target_dir).join("tmp_dataset/eslogs_dataset");
    if !dataset_path.exists() {
        if let Some(parent_dir) = dataset_path.parent() {
            fs::create_dir_all(parent_dir).expect("Failed to create dataset directory");
            tracing::debug!("Created dataset directory: {:?}", parent_dir);
        }
    }
    tracing::info!("Parquet dataset path: {:?}", dataset_path);
    dataset_path
}

/// Generates a random seed for reproducible data generation.
fn generate_seed() -> u64 {
    let mut rng = rand::thread_rng();
    let seed: u32 = rng.gen();
    tracing::debug!("Generated random seed: {}", seed);
    u64::from(seed)
}

/// Checks if the given path is a directory and is empty.
pub fn dataset_exists(base_path: &Path) -> bool {
    let exists = base_path.exists()
        && base_path
            .read_dir()
            .map(|mut i| i.next().is_some())
            .unwrap_or(false);
    tracing::debug!("Dataset exists at {:?}: {}", base_path, exists);
    exists
}

/// Tests the performance of Hive-partitioned S3 Parquet ESLogs.
#[rstest]
async fn test_hive_partitioned_s3_parquet_eslogs_performance(
    #[future] s3: S3,
    mut conn: PgConnection,
    parquet_dataset_path: PathBuf,
) -> Result<()> {
    // Log the start of the test.
    print_utils::init_tracer();
    tracing::info!("Starting test_hive_partitioned_s3_parquet_eslogs_performance");
    tracing::warn!("Path :: {:#?}", parquet_dataset_path);

    // Check if the Parquet file already exists at the specified path.
    if !parquet_dataset_path.exists() || !dataset_exists(&parquet_dataset_path) {
        let seed = generate_seed();
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
            dataset_exists(&parquet_dataset_path)
        );
    }

    // Verify that the Parquet files were created
    if !dataset_exists(&parquet_dataset_path) {
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

    let eslogs_df = EsLogParquetManager::read_parquet_dataset(&parquet_dataset_path)
        .await
        .context("Failed to read ESLogs dataset from local Parquet")?;

    // Await the S3 service setup.
    let s3 = s3.await;

    // Define the S3 bucket name for storing ESLogs data.
    let s3_bucket = "demo-mlp-eslogs";

    // Define the S3 prefix for the ESLogs data
    let s3_prefix = "eslogs_dataset";

    EsLogParquetManager::upload_parquet_dataset_to_s3(
        &s3,
        s3_bucket,
        s3_prefix,
        parquet_dataset_path,
    )
    .await
    .context("Failed to upload Parquet dataset to S3")?;

    s3.print_object_list(s3_bucket, s3_prefix)
        .await
        .context("Failed to list objects in S3")?;

    let foreign_table_id: &str = "eslogs_ft";
    let with_disk_cache: bool = true;

    // Set up the necessary tables in the PostgreSQL database using the data from S3.
    EsLogParquetForeignTableManager::setup_tables(
        &mut conn,
        &s3,
        s3_bucket,
        s3_prefix,
        foreign_table_id,
        with_disk_cache,
    )
    .await?;

    EsLogTestManager::test_count_query(&mut conn, &eslogs_df).await?;
    EsLogTestManager::test_avg_query(&mut conn, &eslogs_df).await?;
    EsLogTestManager::test_group_by_query(&mut conn, &eslogs_df).await?;
    EsLogTestManager::test_time_range_query(&mut conn, &eslogs_df).await?;

    tracing::info!("Successfully read Parquet dataset");
    tracing::info!("Completed test_hive_partitioned_s3_parquet_eslogs_performance");
    Ok(())
}
