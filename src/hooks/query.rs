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

use crate::duckdb::connection;
use crate::fdw::handler::FdwHandler;
use crate::schema::cell::*;
use anyhow::{anyhow, Error, Result};
use duckdb::arrow::array::RecordBatch;
use pgrx::*;
use sqlparser::ast::{BinaryOperator, Expr, Query, SetExpr, Statement, TableFactor, Value};
use sqlparser::dialect::PostgreSqlDialect;
use sqlparser::parser::Parser;
use std::collections::HashSet;
use std::collections::VecDeque;
use std::ffi::CStr;
use std::str::Utf8Error;
use uuid::Uuid;

pub(crate) struct QueryInterceptor;

impl QueryInterceptor {
    pub(crate) fn get_current_query(
        planned_stmt: *mut pg_sys::PlannedStmt,
        query_string: &CStr,
    ) -> Result<String, Utf8Error> {
        pgrx::warning!("pga:: *** get_current_query() X ***");

        let query_start_index = unsafe { (*planned_stmt).stmt_location };
        let query_len = unsafe { (*planned_stmt).stmt_len };
        let full_query = query_string.to_str()?;

        let current_query = if query_start_index != -1 {
            if query_len == 0 {
                full_query[(query_start_index as usize)..full_query.len()].to_string()
            } else {
                full_query[(query_start_index as usize)..((query_start_index + query_len) as usize)]
                    .to_string()
            }
        } else {
            full_query.to_string()
        };

        pgrx::warning!("pga:: *** get_current_query() {:#?} ***", &current_query);

        Ok(current_query)
    }

    pub(crate) fn get_query_relations(planned_stmt: *mut pg_sys::PlannedStmt) -> Vec<PgRelation> {
        let mut relations = Vec::new();

        pgrx::warning!("pga:: *** get_query_relations() X ***");

        unsafe {
            let rtable = (*planned_stmt).rtable;

            if rtable.is_null() {
                return relations;
            }

            #[cfg(feature = "pg12")]
            let mut current_cell = (*rtable).head;
            #[cfg(any(feature = "pg13", feature = "pg14", feature = "pg15", feature = "pg16"))]
            let elements = (*rtable).elements;

            for i in 0..(*rtable).length {
                let rte: *mut pg_sys::RangeTblEntry;
                #[cfg(feature = "pg12")]
                {
                    rte = (*current_cell).data.ptr_value as *mut pg_sys::RangeTblEntry;
                    current_cell = (*current_cell).next;
                }
                #[cfg(any(feature = "pg13", feature = "pg14", feature = "pg15", feature = "pg16"))]
                {
                    rte = (*elements.offset(i as isize)).ptr_value as *mut pg_sys::RangeTblEntry;
                }

                if (*rte).rtekind != pg_sys::RTEKind::RTE_RELATION {
                    continue;
                }
                let relation = pg_sys::RelationIdGetRelation((*rte).relid);
                let pg_relation = PgRelation::from_pg_owned(relation);
                relations.push(pg_relation);
            }
        }

        pgrx::warning!("pga:: *** get_query_relations() {:#?} ***", relations.len());

        relations
    }
}

pub(crate) struct QueryTransformer;

impl QueryTransformer {
    fn get_partitions(parent_oid: pg_sys::Oid) -> Result<Vec<PgRelation>, Error> {
        let mut partitions = Vec::new();
        let mut queue = VecDeque::new();
        queue.push_back(parent_oid);

        while let Some(current_oid) = queue.pop_front() {
            let child_rels = Spi::connect(|client| -> Result<Vec<pg_sys::Oid>, Error> {
                let query = format!(
                    "SELECT inhrelid FROM pg_inherits WHERE inhparent = {}",
                    current_oid.as_u32() // Convert Oid to u32 for proper formatting
                );
                let results = client.select(&query, None, None)?;

                let mut child_oids = Vec::new();
                for row in results {
                    if let Ok(Some(oid)) = row.get::<pg_sys::Oid>(1) {
                        child_oids.push(oid);
                    }
                }
                Ok(child_oids)
            })?;

            for child_oid in child_rels {
                let child_rel = unsafe { PgRelation::open(child_oid) };
                if child_rel.is_partitioned_table() {
                    queue.push_back(child_oid);
                } else if child_rel.is_foreign_table() {
                    partitions.push(child_rel);
                } else {
                    pgrx::warning!("Unexpected table relation kind for OID: {:#?}", child_oid);
                }
            }
        }

        Ok(partitions)
    }

    fn is_valid_fdw(relation: &PgRelation) -> bool {
        unsafe {
            let foreign_table = pg_sys::GetForeignTable(relation.oid());
            let foreign_server = pg_sys::GetForeignServer((*foreign_table).serverid);
            FdwHandler::from(foreign_server) != FdwHandler::Other
        }
    }

    pub(crate) fn transform_query_for_duckdb(
        query: &str,
        query_relations: &[PgRelation],
    ) -> Result<Vec<String>> {
        if query_relations
            .iter()
            .all(|r| r.is_foreign_table() && Self::is_valid_fdw(r))
        {
            Ok(vec![query.to_string()])
        } else {
            let root_table = query_relations
                .iter()
                .find(|r| r.is_partitioned_table())
                .ok_or_else(|| anyhow!("No partitioned table found in the query"))?;

            if !query
                .to_lowercase()
                .contains(&root_table.name().to_lowercase())
            {
                return Err(anyhow!("Root partition table name not found in the query"));
            }

            let all_foreign_tables = Self::get_partitions(root_table.oid())?;
            all_foreign_tables
                .iter()
                .map(|ft| {
                    if !Self::is_valid_fdw(ft) {
                        return Err(anyhow!("Invalid FDW handler for the foreign table"));
                    }
                    Ok(query.replace(root_table.name(), ft.name()))
                })
                .collect()
        }
    }
}

pub(crate) struct QueryExecutor;

impl QueryExecutor {
    fn optimize_transformed_queries(
        original_query: &str,
        transformed_queries: &[String],
    ) -> Result<Vec<String>> {
        pgrx::warning!("pga:: optimize_transformed_queries X");

        let dialect = PostgreSqlDialect {};
        let ast = Parser::parse_sql(&dialect, original_query)?;

        if let Statement::Query(query) = &ast[0] {
            if let SetExpr::Select(select) = query.body.as_ref() {
                if let Some(where_clause) = &select.selection {
                    let keys = Self::extract_keys_from_where_clause(where_clause)?;

                    pgrx::warning!("pga:: optimize_transformed_queries keys :: {:#?}", keys);

                    let optimized_queries =
                        Self::filter_queries_by_keys(transformed_queries, &keys)?;

                    pgrx::warning!("pga:: optimize_transformed_queries",);

                    optimized_queries
                        .iter()
                        .enumerate()
                        .for_each(|(index, entry)| {
                            pgrx::warning!("pga:: {index} => {entry:?}");
                        });

                    if !optimized_queries.is_empty() {
                        return Ok(optimized_queries);
                    }
                }
            }
        }

        pgrx::warning!("pga:: optimize_transformed_queries Y");

        Ok(transformed_queries.to_vec())
    }

    fn extract_keys_from_where_clause(where_clause: &Expr) -> Result<HashSet<String>> {
        let mut keys = HashSet::new();
        Self::extract_values_recursive(where_clause, &mut keys);
        Ok(keys)
    }

    fn extract_values_recursive(expr: &Expr, values: &mut HashSet<String>) {
        match expr {
            Expr::BinaryOp { left, op, right } => {
                match op {
                    BinaryOperator::Eq => {
                        // For equality operations, extract the value
                        if let Some(value) = Self::extract_value(right) {
                            values.insert(value.to_lowercase());
                        }
                    }
                    _ => {
                        // For other operations, continue searching both sides
                        Self::extract_values_recursive(left, values);
                        Self::extract_values_recursive(right, values);
                    }
                }
            }
            Expr::Between { low, high, .. } => {
                if let (Some(low_val), Some(high_val)) =
                    (Self::extract_value(low), Self::extract_value(high))
                {
                    if let (Ok(low_num), Ok(high_num)) =
                        (low_val.parse::<i32>(), high_val.parse::<i32>())
                    {
                        for i in low_num..=high_num {
                            values.insert(i.to_string());
                        }
                    }
                }
            }
            Expr::Nested(nested) => Self::extract_values_recursive(nested, values),
            _ => {}
        }
    }

    fn extract_value(expr: &Expr) -> Option<String> {
        match expr {
            Expr::Value(value) => match value {
                Value::SingleQuotedString(s) | Value::DoubleQuotedString(s) => Some(s.clone()),
                Value::Number(n, _) => Some(n.clone()),
                _ => None,
            },
            _ => None,
        }
    }

    fn filter_queries_by_keys(queries: &[String], keys: &HashSet<String>) -> Result<Vec<String>> {
        queries
            .iter()
            .filter_map(|query| {
                let ast = match Parser::parse_sql(&PostgreSqlDialect {}, query) {
                    Ok(ast) => ast,
                    Err(e) => return Some(Err(anyhow!("SQL parsing error: {}", e))),
                };

                if let Statement::Query(box_query) = &ast[0] {
                    if let SetExpr::Select(select) = &box_query.body.as_ref() {
                        if let Some(table_with_joins) = select.from.first() {
                            if let TableFactor::Table { name, .. } = &table_with_joins.relation {
                                let table_name = name.to_string().to_lowercase();
                                if keys
                                    .iter()
                                    .all(|key| table_name.contains(&key.to_lowercase()))
                                {
                                    return Some(Ok(query.clone()));
                                }
                            }
                        }
                    }
                }
                None
            })
            .collect()
    }

    fn construct_final_query(query: &Query, temp_table_name: &str) -> Result<String> {
        if let SetExpr::Select(_select) = query.body.as_ref() {
            let select_items = Self::get_temp_table_columns(temp_table_name)?;

            let mut final_query = format!("SELECT {} FROM {}", select_items, temp_table_name);

            // TODO :: Have to investigate why "query execution time exceeded the alert threshold (1 second in this case)" ?
            // Extract GROUP BY clause
            // let group_by = match &select.group_by {
            //     GroupByExpr::Expressions(exprs, _) => {
            //         if !exprs.is_empty() {
            //             format!(" GROUP BY {}", exprs.iter().map(|expr| expr.to_string()).collect::<Vec<String>>().join(", "))
            //         } else {
            //             String::new()
            //         }
            //     },
            //     GroupByExpr::All(_) => " GROUP BY ALL".to_string(),
            // };

            // final_query.push_str(&group_by);

            if let Some(order_by) = &query.order_by {
                let order_by_expr = order_by
                    .exprs
                    .iter()
                    .map(|expr| expr.to_string())
                    .collect::<Vec<String>>()
                    .join(", ");
                final_query.push_str(&format!(" ORDER BY {}", order_by_expr));
            }

            if let Some(limit) = &query.limit {
                final_query.push_str(&format!(" LIMIT {}", limit));
            }

            if let Some(offset) = &query.offset {
                final_query.push_str(&format!(" OFFSET {}", offset));
            }

            Ok(final_query)
        } else {
            Err(anyhow!("Unexpected query body type"))
        }
    }

    fn get_temp_table_columns(temp_table_name: &str) -> Result<String> {
        let query = format!(
            "SELECT * FROM information_schema.columns WHERE table_name = '{}'",
            temp_table_name
        );

        unsafe {
            let conn = &mut *connection::get_global_connection().get();
            let mut statement = conn.prepare(&query)?;
            let columns: Vec<String> = statement
                .query_map([], |row| {
                    let column_name: String = row.get("column_name")?;
                    Ok(column_name)
                })?
                .collect::<Result<Vec<_>, _>>()?;

            if columns.is_empty() {
                return Err(anyhow!("No columns found for table {}", temp_table_name));
            }

            Ok(columns.join(", "))
        }
    }

    fn write_batches_to_slots(
        query_desc: PgBox<pg_sys::QueryDesc>,
        mut batches: Vec<RecordBatch>,
    ) -> Result<()> {
        pgrx::warning!(
            "pga:: *** ExtensionHook::write_batches_to_slots() X {:#?} ***",
            batches.len()
        );

        // Convert the DataFusion batches to Postgres tuples and send them to the destination
        unsafe {
            let tuple_desc = PgTupleDesc::from_pg(query_desc.tupDesc);
            let estate = query_desc.estate;
            (*estate).es_processed = 0;

            let dest = query_desc.dest;
            let startup = (*dest)
                .rStartup
                .ok_or_else(|| anyhow!("rStartup not found"))?;
            startup(dest, query_desc.operation as i32, query_desc.tupDesc);

            let receive = (*dest)
                .receiveSlot
                .ok_or_else(|| anyhow!("receiveSlot not found"))?;

            for batch in batches.iter_mut() {
                for row_index in 0..batch.num_rows() {
                    let tuple_table_slot =
                        pg_sys::MakeTupleTableSlot(query_desc.tupDesc, &pg_sys::TTSOpsVirtual);

                    pg_sys::ExecStoreVirtualTuple(tuple_table_slot);

                    for (col_index, _) in tuple_desc.iter().enumerate() {
                        let attribute = tuple_desc.get(col_index).ok_or_else(|| {
                            anyhow!("attribute at {col_index} not found in tupdesc")
                        })?;
                        let column = batch.column(col_index);
                        let tts_value = (*tuple_table_slot).tts_values.add(col_index);
                        let tts_isnull = (*tuple_table_slot).tts_isnull.add(col_index);

                        match column.get_cell(row_index, attribute.atttypid, attribute.name())? {
                            Some(cell) => {
                                if let Some(datum) = cell.into_datum() {
                                    *tts_value = datum;
                                }
                            }
                            None => {
                                *tts_isnull = true;
                            }
                        };
                    }

                    receive(tuple_table_slot, dest);
                    (*estate).es_processed += 1;
                    pg_sys::ExecDropSingleTupleTableSlot(tuple_table_slot);
                }
            }

            let shutdown = (*dest)
                .rShutdown
                .ok_or_else(|| anyhow!("rShutdown not found"))?;
            shutdown(dest);
        }

        pgrx::warning!("pga:: *** ExtensionHook::write_batches_to_slots() Y ***");

        Ok(())
    }

    pub(crate) fn execute_multi_query(
        query_desc: &PgBox<pg_sys::QueryDesc>,
        transformed_queries: &[String],
        original_query: &str,
        _query_relations: &[PgRelation],
    ) -> Result<()> {
        let optimized_transformed_queries =
            Self::optimize_transformed_queries(original_query, transformed_queries)?;

        pgrx::warning!(
            "pga:: transformed_queries :: {:#?} \n optimized_transformed_queries :: {:#?}",
            transformed_queries.len(),
            optimized_transformed_queries.len()
        );

        #[allow(clippy::single_char_pattern)]
        let temp_table_name = format!(
            "temp_result_cache_{}",
            Uuid::new_v4().to_string().replace("-", "_")
        );

        // Create a temporary table to store intermediate results
        connection::execute(
            &format!(
                "CREATE TEMPORARY TABLE {} AS SELECT * FROM ({}) WHERE 1=0",
                temp_table_name, transformed_queries[0]
            ),
            [],
        )?;

        // Execute each query and insert results into the temporary table
        for query in optimized_transformed_queries {
            connection::execute(&format!("INSERT INTO {} {}", temp_table_name, query), [])?;
        }

        // Parse the original query
        let dialect = PostgreSqlDialect {};
        let ast = Parser::parse_sql(&dialect, original_query)?;

        if let Statement::Query(query) = &ast[0] {
            let final_query = Self::construct_final_query(query, &temp_table_name)?;

            // Execute the final query and write results
            connection::create_arrow(&final_query)?;
            let batches = connection::get_batches()?;
            Self::write_batches_to_slots(query_desc.clone(), batches)?;
        } else {
            return Err(anyhow!("Unexpected SQL statement type"));
        }

        // Clean up
        connection::execute(&format!("DROP TABLE IF EXISTS {}", temp_table_name), [])?;
        connection::clear_arrow();

        Ok(())
    }

    pub(crate) fn execute_duckdb_query(
        query_desc: &PgBox<pg_sys::QueryDesc>,
        query: &str,
    ) -> Result<()> {
        connection::create_arrow(query)?;
        let batches = connection::get_batches()?;
        Self::write_batches_to_slots(query_desc.clone(), batches)?;
        connection::clear_arrow();
        Ok(())
    }

    pub(crate) fn should_use_duckdb(
        query_desc: &PgBox<pg_sys::QueryDesc>,
        query: &str,
        query_relations: &[PgRelation],
    ) -> bool {
        !query_relations.is_empty()
            && query_relations.iter().any(|pg_relation| {
                pg_relation.is_foreign_table() || pg_relation.is_partitioned_table()
            })
            && query_desc.operation == pg_sys::CmdType::CMD_SELECT
            && !query.to_lowercase().starts_with("copy")
            && !query.to_lowercase().starts_with("create")
    }
}
