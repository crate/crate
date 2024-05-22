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

import static io.crate.testing.Asserts.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.MapperTestUtils;
import org.junit.Test;

import io.crate.exceptions.ColumnUnknownException;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.IndexType;
import io.crate.metadata.PartitionName;
import io.crate.metadata.Reference;
import io.crate.metadata.ReferenceIdent;
import io.crate.metadata.RelationName;
import io.crate.metadata.RowGranularity;
import io.crate.metadata.SimpleReference;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.metadata.doc.DocTableInfoFactory;
import io.crate.sql.tree.ColumnPolicy;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.IndexEnv;
import io.crate.testing.SQLExecutor;
import io.crate.types.DataTypes;

public class RenameColumnTaskTest extends CrateDummyClusterServiceUnitTest {

    @Test
    public void test_rename_simple_columns() throws Exception {
        // the test is equivalent to : alter table tbl rename column x to y;
        var e = SQLExecutor.of(clusterService)
            .addTable("create table tbl (x int)");
        DocTableInfo tbl = e.resolveTableInfo("tbl");
        try (IndexEnv indexEnv = new IndexEnv(
            THREAD_POOL,
            tbl,
            clusterService.state(),
            Version.CURRENT
        )) {
            var renameColumnTask = new AlterTableTask<>(
                e.nodeCtx, imd -> indexEnv.mapperService(), tbl.ident(), TransportRenameColumnAction.RENAME_COLUMN_OPERATOR);
            Reference refToRename = tbl.getReference(new ColumnIdent("x"));
            var newName = new ColumnIdent("y");
            var request = new RenameColumnRequest(tbl.ident(), refToRename, newName);
            ClusterState newState = renameColumnTask.execute(clusterService.state(), request);
            DocTableInfo newTable = new DocTableInfoFactory(e.nodeCtx).create(tbl.ident(), newState.metadata());
            assertThat(newTable.getReference(refToRename.column())).isNull();
            assertThat(newTable.getReference(newName)).isReference().hasName(newName.sqlFqn());
        }
    }

    @Test
    public void test_rename_nested_columns() throws Exception {
        // equivalent to:
        // alter table tbl rename column o to p;
        // alter table tbl rename column p['o2'] to p['p2'];
        // alter table tbl rename column p['p2']['x'] to p['p2']['y'];
        var e = SQLExecutor.of(clusterService)
            .addTable("create table tbl (o object as (o2 object as (x int)))");
        DocTableInfo tbl = e.resolveTableInfo("tbl");
        try (IndexEnv indexEnv = new IndexEnv(
            THREAD_POOL,
            tbl,
            clusterService.state(),
            Version.CURRENT
        )) {
            var renameColumnTask = new AlterTableTask<>(
                e.nodeCtx, imd -> indexEnv.mapperService(), tbl.ident(), TransportRenameColumnAction.RENAME_COLUMN_OPERATOR);
            Reference refToRename1 = tbl.getReference(new ColumnIdent("o"));
            var newName1 = new ColumnIdent("p");
            var request = new RenameColumnRequest(tbl.ident(), refToRename1, newName1);
            ClusterState newState = renameColumnTask.execute(clusterService.state(), request);
            tbl = new DocTableInfoFactory(e.nodeCtx).create(tbl.ident(), newState.metadata());
            assertThat(tbl.getReference(refToRename1.column())).isNull();
            assertThat(tbl.getReference(newName1)).isReference().hasName(newName1.sqlFqn());

            Reference refToRename2 = tbl.getReference(new ColumnIdent("p", List.of("o2")));
            var newName2 = new ColumnIdent("p", List.of("p2"));
            request = new RenameColumnRequest(tbl.ident(), refToRename2, newName2);
            newState = renameColumnTask.execute(newState, request);
            tbl = new DocTableInfoFactory(e.nodeCtx).create(tbl.ident(), newState.metadata());
            assertThat(tbl.getReference(refToRename2.column())).isNull();
            assertThat(tbl.getReference(newName2)).isReference().hasName(newName2.sqlFqn());

            Reference refToRename3 = tbl.getReference(new ColumnIdent("p", List.of("p2", "x")));
            var newName3 = new ColumnIdent("p", List.of("p2", "y"));
            request = new RenameColumnRequest(tbl.ident(), refToRename3, newName3);
            newState = renameColumnTask.execute(newState, request);
            tbl = new DocTableInfoFactory(e.nodeCtx).create(tbl.ident(), newState.metadata());
            assertThat(tbl.getReference(refToRename3.column())).isNull();
            assertThat(tbl.getReference(newName3)).isReference().hasName(newName3.sqlFqn());
        }
    }

    @Test
    public void test_rename_partitioned_by_columns() throws Exception {
        // alter table tbl rename column x to y; -- where y is a partitioned by col
        var e = SQLExecutor.of(clusterService)
            .addPartitionedTable("create table doc.tbl (o object as (o2 object as (x int)), x int) partitioned by (o['o2']['x'], x)",
                new PartitionName(new RelationName("doc", "tbl"), List.of("1", "2")).asIndexName(),
                new PartitionName(new RelationName("doc", "tbl"), List.of("3", "4")).asIndexName());
        DocTableInfo tbl = e.resolveTableInfo("doc.tbl");
        var renameColumnTask = new AlterTableTask<>(e.nodeCtx, imd -> MapperTestUtils.newMapperService(
            createTempDir(),
            Settings.EMPTY,
            "doc.tbl"),
            tbl.ident(),
            TransportRenameColumnAction.RENAME_COLUMN_OPERATOR);
        Reference refToRename = tbl.getReference(new ColumnIdent("x"));
        var newName = new ColumnIdent("y");
        var request = new RenameColumnRequest(tbl.ident(), refToRename, newName);
        ClusterState newState = renameColumnTask.execute(clusterService.state(), request);
        DocTableInfo newTable = new DocTableInfoFactory(e.nodeCtx).create(tbl.ident(), newState.metadata());
        assertThat(newTable.getReference(refToRename.column())).isNull();
        assertThat(newTable.partitionedBy()).doesNotContain(refToRename.column());
        assertThat(newTable.getReference(newName)).isReference().hasName(newName.name());
        assertThat(newTable.partitionedBy()).doesNotContain(refToRename.column()).contains(newName);

        // alter table tbl rename column o to p;
        // -- causes partitioned by col to be renamed: o['o2']['x'] -> p['o2']['x']
        refToRename = tbl.getReference(new ColumnIdent("o"));
        newName = new ColumnIdent("p");
        var oldNameOfChild = new ColumnIdent("o", List.of("o2", "x"));
        var newNameOfChild = new ColumnIdent("p", List.of("o2", "x"));
        request = new RenameColumnRequest(tbl.ident(), refToRename, newName);
        newState = renameColumnTask.execute(newState, request);
        newTable = new DocTableInfoFactory(e.nodeCtx).create(tbl.ident(), newState.metadata());
        assertThat(newTable.getReference(oldNameOfChild)).isNull();
        assertThat(newTable.getReference(newName)).isReference().hasName(newNameOfChild.name());
        assertThat(newTable.partitionedBy()).doesNotContain(oldNameOfChild).contains(newNameOfChild);
    }

    @Test
    public void test_rename_primary_key_columns() throws Exception {
        // alter table tbl rename column o['o2'] to o['p2'];
        // -- causes primary key column to be renamed: o['o2']['o3']['x'] -> o['p2']['o3']['x']
        var e = SQLExecutor.of(clusterService)
            .addTable("create table tbl (o object as (o2 object as (o3 object as (x int primary key))), a int primary key)");
        DocTableInfo tbl = e.resolveTableInfo("tbl");
        try (IndexEnv indexEnv = new IndexEnv(
            THREAD_POOL,
            tbl,
            clusterService.state(),
            Version.CURRENT
        )) {
            var renameColumnTask = new AlterTableTask<>(
                e.nodeCtx, imd -> indexEnv.mapperService(), tbl.ident(), TransportRenameColumnAction.RENAME_COLUMN_OPERATOR);
            Reference refToRename = tbl.getReference(new ColumnIdent("o", List.of("o2")));
            var newName = new ColumnIdent("o", List.of("p2"));
            var request = new RenameColumnRequest(tbl.ident(), refToRename, newName);
            ClusterState newState = renameColumnTask.execute(clusterService.state(), request);
            DocTableInfo newTable = new DocTableInfoFactory(e.nodeCtx).create(tbl.ident(), newState.metadata());
            var oldPkCol = new ColumnIdent("o", List.of("o2", "o3", "x"));
            var newPkCol = new ColumnIdent("o", List.of("p2", "o3", "x"));
            assertThat(newTable.getReference(refToRename.column())).isNull();
            assertThat(newTable.getReference(newName)).isReference().hasName(newName.sqlFqn());
            assertThat(newTable.primaryKey()).doesNotContain(oldPkCol).contains(newPkCol);
        }
    }

    @Test
    public void test_rename_check_columns() throws Exception {
        // alter table tbl rename column o['a'] to o['b'];
        // alter table tbl rename column o to p;
        // -- causes check constraint to be renamed: (o['a'] > 1) -> (p['b'] > 1)
        var e = SQLExecutor.of(clusterService)
            .addTable("create table tbl (o object as (a int), constraint c_1 check (o['a'] > 1))");
        DocTableInfo tbl = e.resolveTableInfo("tbl");
        try (IndexEnv indexEnv = new IndexEnv(
            THREAD_POOL,
            tbl,
            clusterService.state(),
            Version.CURRENT
        )) {
            var renameColumnTask = new AlterTableTask<>(
                e.nodeCtx, imd -> indexEnv.mapperService(), tbl.ident(), TransportRenameColumnAction.RENAME_COLUMN_OPERATOR);
            Reference refToRename = tbl.getReference(new ColumnIdent("o", List.of("a")));
            var newName = new ColumnIdent("o", List.of("b"));
            var request = new RenameColumnRequest(tbl.ident(), refToRename, newName);
            ClusterState newState = renameColumnTask.execute(clusterService.state(), request);
            tbl = new DocTableInfoFactory(e.nodeCtx).create(tbl.ident(), newState.metadata());

            refToRename = tbl.getReference(new ColumnIdent("o"));
            newName = new ColumnIdent("p");
            request = new RenameColumnRequest(tbl.ident(), refToRename, newName);
            newState = renameColumnTask.execute(newState, request);
            tbl = new DocTableInfoFactory(e.nodeCtx).create(tbl.ident(), newState.metadata());

            assertThat(tbl.checkConstraints()).hasSize(1);
            assertThat(tbl.checkConstraints().getFirst().toString())
                .isEqualTo("CheckConstraint{name='c_1', expression=(p['b'] > 1)}");
        }
    }

    @Test
    public void test_rename_columns_used_in_generated_expressions() throws Exception {
        // alter table tbl rename column o to o2;
        // -- causes generated column to be renamed: (o['b'] AS o['a'] + 1) -> (o2['b'] AS o2['a'] + 1)
        var e = SQLExecutor.of(clusterService)
            .addTable("create table tbl (o object as (a int, b int generated always as (o['a']+1)))");
        DocTableInfo tbl = e.resolveTableInfo("tbl");
        try (IndexEnv indexEnv = new IndexEnv(
            THREAD_POOL,
            tbl,
            clusterService.state(),
            Version.CURRENT
        )) {
            var renameColumnTask = new AlterTableTask<>(
                e.nodeCtx, imd -> indexEnv.mapperService(), tbl.ident(), TransportRenameColumnAction.RENAME_COLUMN_OPERATOR);
            Reference refToRename = tbl.getReference(new ColumnIdent("o"));
            var newName = new ColumnIdent("o2");
            var request = new RenameColumnRequest(tbl.ident(), refToRename, newName);
            ClusterState newState = renameColumnTask.execute(clusterService.state(), request);
            DocTableInfo newTable = new DocTableInfoFactory(e.nodeCtx).create(tbl.ident(), newState.metadata());
            assertThat(newTable.generatedColumns()).hasSize(1);
            assertThat(newTable.generatedColumns().getFirst().column()).isEqualTo(new ColumnIdent("o2", List.of("b")));
            assertThat(newTable.generatedColumns().getFirst().formattedGeneratedExpression()).isEqualTo("(o2['a'] + 1)");
        }
    }

    @Test
    public void test_cannot_rename_column_to_name_in_use() throws Exception {
        var e = SQLExecutor.of(clusterService)
            .addTable("create table tbl (a int, b int)");
        DocTableInfo tbl = e.resolveTableInfo("tbl");
        try (IndexEnv indexEnv = new IndexEnv(
            THREAD_POOL,
            tbl,
            clusterService.state(),
            Version.CURRENT
        )) {
            var renameColumnTask = new AlterTableTask<>(
                e.nodeCtx, imd -> indexEnv.mapperService(), tbl.ident(), TransportRenameColumnAction.RENAME_COLUMN_OPERATOR);
            Reference refToRename = tbl.getReference(new ColumnIdent("a"));
            var newName = new ColumnIdent("b");
            var request = new RenameColumnRequest(tbl.ident(), refToRename, newName);
            assertThatThrownBy(() -> renameColumnTask.execute(clusterService.state(), request))
                .isExactlyInstanceOf(IllegalArgumentException.class)
                .hasMessage("Cannot rename column to a name that is in use");
        }
    }

    @Test
    public void test_cannot_rename_column_if_the_column_has_changed() throws Exception {
        var e = SQLExecutor.of(clusterService)
            .addTable("create table tbl (a int)");
        DocTableInfo tbl = e.resolveTableInfo("tbl");
        try (IndexEnv indexEnv = new IndexEnv(
            THREAD_POOL,
            tbl,
            clusterService.state(),
            Version.CURRENT
        )) {
            var renameColumnTask = new AlterTableTask<>(
                e.nodeCtx, imd -> indexEnv.mapperService(), tbl.ident(), TransportRenameColumnAction.RENAME_COLUMN_OPERATOR);
            SimpleReference refToRename = new SimpleReference(
                new ReferenceIdent(tbl.ident(), "a"),
                RowGranularity.DOC,
                // since the analysis, the data type of 'a' has changed from int to double
                DataTypes.DOUBLE,
                ColumnPolicy.DYNAMIC,
                IndexType.PLAIN,
                true,
                true,
                1,
                1,
                false,
                null
            );
            var newName = new ColumnIdent("b");
            var request = new RenameColumnRequest(tbl.ident(), refToRename, newName);
            assertThatThrownBy(() -> renameColumnTask.execute(clusterService.state(), request))
                .isExactlyInstanceOf(ColumnUnknownException.class)
                .hasMessage("Column a unknown");
        }
    }

    @Test
    public void test_rename_clustered_by_columns() throws Exception {
        var e = SQLExecutor.of(clusterService)
            .addTable("create table tbl (x int) clustered by (x)");
        DocTableInfo tbl = e.resolveTableInfo("tbl");
        try (IndexEnv indexEnv = new IndexEnv(
            THREAD_POOL,
            tbl,
            clusterService.state(),
            Version.CURRENT
        )) {
            var renameColumnTask = new AlterTableTask<>(
                e.nodeCtx, imd -> indexEnv.mapperService(), tbl.ident(), TransportRenameColumnAction.RENAME_COLUMN_OPERATOR);
            Reference refToRename = tbl.getReference(new ColumnIdent("x"));
            var newName = new ColumnIdent("y");
            var request = new RenameColumnRequest(tbl.ident(), refToRename, newName);
            ClusterState newState = renameColumnTask.execute(clusterService.state(), request);
            DocTableInfo newTable = new DocTableInfoFactory(e.nodeCtx).create(tbl.ident(), newState.metadata());
            assertThat(newTable.clusteredBy()).isEqualTo(newName);
        }
    }

    @Test
    public void test_rename_index_column() throws Exception {
        var e = SQLExecutor.of(clusterService)
            .addTable("create table tbl (x text, index i using fulltext (x))");
        DocTableInfo tbl = e.resolveTableInfo("tbl");
        try (IndexEnv indexEnv = new IndexEnv(
            THREAD_POOL,
            tbl,
            clusterService.state(),
            Version.CURRENT
        )) {
            var renameColumnTask = new AlterTableTask<>(
                e.nodeCtx, imd -> indexEnv.mapperService(), tbl.ident(), TransportRenameColumnAction.RENAME_COLUMN_OPERATOR);
            Reference refToRename = tbl.getReference(new ColumnIdent("x"));
            var newName = new ColumnIdent("x2");
            var request = new RenameColumnRequest(tbl.ident(), refToRename, newName);
            ClusterState newState = renameColumnTask.execute(clusterService.state(), request);
            tbl = new DocTableInfoFactory(e.nodeCtx).create(tbl.ident(), newState.metadata());

            refToRename = tbl.indexColumn(new ColumnIdent("i"));
            newName = new ColumnIdent("i2");
            request = new RenameColumnRequest(tbl.ident(), refToRename, newName);
            newState = renameColumnTask.execute(newState, request);
            tbl = new DocTableInfoFactory(e.nodeCtx).create(tbl.ident(), newState.metadata());

            var renamedIndexColumn = tbl.indexColumn(new ColumnIdent("i2"));
            assertThat(renamedIndexColumn.columns()).hasSize(1);
            assertThat(renamedIndexColumn.columns().getFirst()).isReference().hasName("x2");
            assertThat(renamedIndexColumn.column()).isEqualTo(new ColumnIdent("i2"));
        }
    }
}
