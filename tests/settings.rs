mod fixtures;

use anyhow::Result;
use fixtures::conn;
use fixtures::db::Query;
use fixtures::print_utils;
use rstest::*;
use sqlx::PgConnection;

#[rstest]
async fn test_duckdb_global_settings(mut conn: PgConnection) -> Result<()> {
    print_utils::init_tracer();

    tracing::error!("Executing the Test:: test_duckdb_settings");

    // let global_settings: Vec<(Option<String>, Option<String>, Option<String>, Option<String>, Option<String>)> =
    //     "SELECT * FROM duckdb_settings()".fetch(&mut conn);

    let global_settings: (Option<String>,) =
        "SELECT value FROM duckdb_settings() WHERE name='enable_object_cache'".fetch_one(&mut conn);

    tracing::error!("global_settings:: {:#?}", global_settings);

    "SELECT duckdb_execute($$SET enable_object_cache='true'$$)".execute(&mut conn);

    let global_settings: (Option<String>,) =
        "SELECT value FROM duckdb_settings() WHERE name='enable_object_cache'".fetch_one(&mut conn);

    tracing::error!("global_settings:: {:#?}", global_settings);

    Ok(())
}

// #[rstest]
// async fn test_duckdb_settings(mut conn: PgConnection) -> Result<()> {
//     print_utils::init_tracer();

//     tracing::error!("Executing the Test:: test_duckdb_settings");

//     "SELECT duckdb_execute($$SET memory_limit='10GiB'$$)".execute(&mut conn);
//     let memory_limit: (Option<String>,) =
//         "SELECT value FROM duckdb_settings() WHERE name='memory_limit'".fetch_one(&mut conn);
//     assert_eq!(memory_limit.0, Some("10.0 GiB".to_string()));

//     Ok(())
// }
