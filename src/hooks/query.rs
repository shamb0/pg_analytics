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

use crate::fdw::handler::FdwHandler;
use anyhow::{anyhow, Error, Result};
use pgrx::*;
use std::collections::VecDeque;
use std::ffi::CStr;
use std::str::Utf8Error;

pub fn get_current_query(
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

pub fn get_query_relations(planned_stmt: *mut pg_sys::PlannedStmt) -> Vec<PgRelation> {
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

pub fn transform_query_for_duckdb(
    query: &str,
    query_relations: &[PgRelation],
) -> Result<Vec<String>> {
    if query_relations
        .iter()
        .all(|r| r.is_foreign_table() && is_valid_fdw(r))
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

        let all_foreign_tables = get_partitions(root_table.oid())?;
        all_foreign_tables
            .iter()
            .map(|ft| {
                if !is_valid_fdw(ft) {
                    return Err(anyhow!("Invalid FDW handler for the foreign table"));
                }
                Ok(query.replace(root_table.name(), ft.name()))
            })
            .collect()
    }
}

// pub fn transform_query_for_duckdb(query: &str, query_relations: &[PgRelation]) -> Result<String> {
//     let mut transformed_query = query.to_string();

//     // Find the root partitioned table and the corresponding foreign table
//     let root_table = query_relations.iter().find(|r| r.is_partitioned_table());
//     let foreign_table = query_relations.iter().find(|r| r.is_foreign_table());

//     if let (Some(root), Some(foreign)) = (root_table, foreign_table) {
//         // Check if the root partition table name is in the query
//         if !query.to_lowercase().contains(&root.name().to_lowercase()) {
//             return Err(anyhow!("Root partition table name not found in the query"));
//         }

//         // Check if the foreign table uses a valid FDW handler
//         let foreign_table = unsafe { pg_sys::GetForeignTable(foreign.oid()) };
//         let foreign_server = unsafe { pg_sys::GetForeignServer((*foreign_table).serverid) };
//         let fdw_handler = FdwHandler::from(foreign_server);
//         if fdw_handler == FdwHandler::Other {
//             return Err(anyhow!("Invalid FDW handler for the foreign table"));
//         }

//         // Replace the root table name with the foreign table name in the query
//         transformed_query = transformed_query.replace(&root.name(), &foreign.name());

//         // Add any necessary transformations here, such as modifying WHERE clauses
//         // to include partition key conditions
//     } else {
//         return Err(anyhow!("Required tables not found in query relations"));
//     }

//     Ok(transformed_query)
// }
