/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations
 * under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial agreement.
 */

/// There are four types of write operations from a user perspective:
///
/// 1. INSERT INTO
/// 2. INSERT INTO with ON CONFLICT
/// 3. COPY TO
/// 4. UPDATE
///
/// Internally all four use [io.crate.execution.dml.upsert.ShardUpsertRequest]
/// With either:
///
/// - Only `insertColumns` set (INSERT INTO or COPY TO, both pure insert)
/// - Only `updateColumns` set (pure update)
/// - Both `insertColumns` and `updateColumns` set (INSERT with ON CONFLICT)
///
/// Given that Lucene is append-only, update operations are executed with a read
/// followed by a write using:
///
/// - [io.crate.execution.engine.collect.PKLookupOperation] for the read
/// - [io.crate.execution.dml.upsert.UpdateToInsert] to convert the read data into a structure for the insert
/// - [io.crate.execution.dml.Indexer] to create the Lucene index fields
///
/// The read and update->insert conversion always happens on the primary shard.
/// For the replica, the operation always looks like a pure insert.
/// Pure inserts use [io.crate.execution.dml.Indexer] directly.
///
/// ON CONFLICT are internally either like an insert or an update.
/// This means that with bulk operations there can be a mix of inserts and updates within the same statement.
///
/// For the replica request the `insertColumns` will be the superset of the
/// insert and update cases.
/// The values on item level can have different lengths to cover either the
/// insert (fewer values) or update (more values).
///
/// Given a table like:
///
/// ```sql
/// CREATE TABLE tbl (
///     id int primary key,
///     x int,
///     y int,
///     z int default 0,
///     write_ts as current_timestamp
/// )
/// ```
///
/// And a statement like:
///
/// ```sql
/// INSERT INTO tbl (id, x) VALUES (?, ?)
///     ON CONFLICT (id) DO UPDATE SET y = 10
/// ```
///
/// We could have:
///
/// - INSERT [id, x, write_ts]       with VALUES [?, ?, write_ts generated]
/// - UPDATE [id, x, write_ts, z, y] with VALUES [?, ?, write_ts generated, z, y]
///
/// The column order for the replica is always:
///
/// - Insert target columns
/// - Undeterministic generated columns (always added to write the same value on the replica)
/// - All remaining table columns (for update or on conflict)
///
package io.crate.execution.dml.upsert;
