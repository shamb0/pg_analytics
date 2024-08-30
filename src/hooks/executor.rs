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

use anyhow::Result;
use pgrx::*;
use std::ffi::CStr;

use super::query::*;

macro_rules! fallback_warning {
    ($msg:expr) => {
        warning!("This query was not fully pushed down to DuckDB because DuckDB returned an error. Query times may be impacted. If you would like to see this query pushed down, please submit a request to https://github.com/paradedb/paradedb/issues with the following context:\n{}", $msg);
    };
}

#[allow(deprecated)]
pub async fn executor_run(
    query_desc: PgBox<pg_sys::QueryDesc>,
    direction: pg_sys::ScanDirection::Type,
    count: u64,
    execute_once: bool,
    prev_hook: fn(
        query_desc: PgBox<pg_sys::QueryDesc>,
        direction: pg_sys::ScanDirection::Type,
        count: u64,
        execute_once: bool,
    ) -> HookResult<()>,
) -> Result<()> {
    pgrx::warning!("pga:: *** ExtensionHook::executor_run() X ***");

    let ps = query_desc.plannedstmt;
    let query =
        QueryInterceptor::get_current_query(ps, unsafe { CStr::from_ptr(query_desc.sourceText) })?;
    let query_relations = QueryInterceptor::get_query_relations(ps);

    pgrx::warning!(
        "query_relations.is_empty() :: {:#?}",
        query_relations.is_empty()
    );

    if !QueryExecutor::should_use_duckdb(&query_desc, &query, &query_relations) {
        prev_hook(query_desc, direction, count, execute_once);
        pgrx::warning!("pga:: *** ExtensionHook::executor_run() Y Stg-01 ***");
        return Ok(());
    }

    match QueryTransformer::transform_query_for_duckdb(&query, &query_relations) {
        Ok(transformed_queries) => {
            if transformed_queries.len() > 1 {
                QueryExecutor::execute_multi_query(
                    &query_desc,
                    &transformed_queries,
                    &query,
                    &query_relations,
                )?;
            } else {
                QueryExecutor::execute_duckdb_query(&query_desc, &transformed_queries[0])?;
            }
        }
        Err(err) => {
            fallback_warning!(err.to_string());
            prev_hook(query_desc, direction, count, execute_once);
            pgrx::warning!(
                "pga:: *** ExtensionHook::executor_run() Y transform error {:#?} ***",
                err
            );
            return Ok(());
        }
    }

    pgrx::warning!("pga:: *** ExtensionHook::executor_run() Y ***");
    Ok(())
}
