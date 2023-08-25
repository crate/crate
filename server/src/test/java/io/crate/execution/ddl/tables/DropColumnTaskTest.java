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

package io.crate.execution.ddl.tables;

import static io.crate.analyze.AnalyzedAlterTableDropColumn.DropColumn;
import static io.crate.testing.Asserts.assertThat;

import java.util.List;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterState;
import org.junit.Test;

import io.crate.metadata.ReferenceIdent;
import io.crate.metadata.RowGranularity;
import io.crate.metadata.SimpleReference;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.metadata.doc.DocTableInfoFactory;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.IndexEnv;
import io.crate.testing.SQLExecutor;
import io.crate.types.DataTypes;

public class DropColumnTaskTest extends CrateDummyClusterServiceUnitTest {

    @Test
    public void test_can_drop_simple_column() throws Exception {
        var e = SQLExecutor.builder(clusterService)
            .addTable("create table tbl (x int, y int)")
            .build();
        DocTableInfo tbl = e.resolveTableInfo("tbl");
        try (IndexEnv indexEnv = new IndexEnv(
            THREAD_POOL,
            tbl,
            clusterService.state(),
            Version.CURRENT,
            createTempDir()
        )) {
            var dropColumnTask = new DropColumnTask(e.nodeCtx, imd -> indexEnv.mapperService());
            ReferenceIdent refIdent = new ReferenceIdent(tbl.ident(), "y");
            SimpleReference colToDrop = new SimpleReference(
                refIdent,
                RowGranularity.DOC,
                DataTypes.SHORT, // irrelevant
                333, // irrelevant
                null
            );
            var request = new DropColumnRequest(tbl.ident(), List.of(new DropColumn(colToDrop, false)));
            ClusterState newState = dropColumnTask.execute(clusterService.state(), request);
            DocTableInfo newTable = new DocTableInfoFactory(e.nodeCtx).create(tbl.ident(), newState);

            assertThat(newTable.getReference(colToDrop.column())).isNull();
        }
    }

    @Test
    public void test_can_drop_subcolumn() throws Exception {
        var e = SQLExecutor.builder(clusterService)
            .addTable("create table tbl (id int, o object AS(a int, b int, oo object AS (a int, b int)))")
            .build();
        DocTableInfo tbl = e.resolveTableInfo("tbl");
        try (IndexEnv indexEnv = new IndexEnv(
            THREAD_POOL,
            tbl,
            clusterService.state(),
            Version.CURRENT,
            createTempDir()
        )) {
            var dropColumnTask = new DropColumnTask(e.nodeCtx, imd -> indexEnv.mapperService());
            ReferenceIdent refIdent = new ReferenceIdent(tbl.ident(), "o", List.of("oo"));
            SimpleReference colToDrop = new SimpleReference(
                refIdent,
                RowGranularity.DOC,
                DataTypes.SHORT, // irrelevant
                333, // irrelevant
                null
            );
            var request = new DropColumnRequest(tbl.ident(), List.of(new DropColumn(colToDrop, false)));
            ClusterState newState = dropColumnTask.execute(clusterService.state(), request);
            DocTableInfo newTable = new DocTableInfoFactory(e.nodeCtx).create(tbl.ident(), newState);

            assertThat(newTable.getReference(colToDrop.column())).isNull();
        }
    }

    @Test
    public void test_is_no_op_if_columns_exist() throws Exception {
        var e = SQLExecutor.builder(clusterService)
            .addTable("create table tbl (x int, y int)")
            .build();
        DocTableInfo tbl = e.resolveTableInfo("tbl");
        ClusterState state = clusterService.state();
        try (IndexEnv indexEnv = new IndexEnv(
            THREAD_POOL,
            tbl,
            state,
            Version.CURRENT,
            createTempDir()
        )) {
            var dropColumnTask = new DropColumnTask(e.nodeCtx, imd -> indexEnv.mapperService());
            ReferenceIdent refIdent = new ReferenceIdent(tbl.ident(), "z");
            SimpleReference colToDrop = new SimpleReference(
                refIdent,
                RowGranularity.DOC,
                DataTypes.INTEGER,
                333, // irrelevant
                null
            );
            var request = new DropColumnRequest(tbl.ident(), List.of(new DropColumn(colToDrop, true)));
            ClusterState newState = dropColumnTask.execute(state, request);
            assertThat(newState).isSameAs(state);
        }
    }
}
