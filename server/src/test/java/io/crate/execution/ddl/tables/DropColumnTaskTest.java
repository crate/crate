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
import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import java.util.Map;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterModule;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.index.MapperTestUtils;
import org.junit.Test;

import io.crate.analyze.DropColumn;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.PartitionName;
import io.crate.metadata.Reference;
import io.crate.metadata.ReferenceIdent;
import io.crate.metadata.RelationName;
import io.crate.metadata.RowGranularity;
import io.crate.metadata.SimpleReference;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.metadata.doc.DocTableInfoFactory;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.IndexEnv;
import io.crate.testing.SQLExecutor;
import io.crate.types.DataTypes;
import io.crate.types.ObjectType;

public class DropColumnTaskTest extends CrateDummyClusterServiceUnitTest {

    @Test
    public void test_can_drop_simple_column() throws Exception {
        var e = SQLExecutor.of(clusterService)
            .addTable("create table tbl (x int, y int, z int)");
        DocTableInfo tbl = e.resolveTableInfo("tbl");
        ClusterState initialState = clusterService.state();
        try (IndexEnv indexEnv = new IndexEnv(
            THREAD_POOL,
            tbl,
            initialState,
            Version.CURRENT
        )) {
            var dropColumnTask = new AlterTableTask<>(
                e.nodeCtx, imd -> indexEnv.mapperService(), tbl.ident(), TransportDropColumnAction.DROP_COLUMN_OPERATOR);
            Reference colToDrop = tbl.getReference(new ColumnIdent("y"));
            var request = new DropColumnRequest(tbl.ident(), List.of(new DropColumn(colToDrop, false)));
            ClusterState newState = dropColumnTask.execute(initialState, request);

            assertThat(newState.version())
                .as("Version is not increased. The MasterService does when the state is applied")
                .isEqualTo(initialState.version());
            assertThat(newState.metadata().version())
                .as("Version is not increased. The MasterService does when the state is applied")
                .isEqualTo(initialState.metadata().version());

            String indexName = tbl.ident().indexNameOrAlias();
            assertThat(newState.metadata().index(indexName).getVersion())
                .isGreaterThan(initialState.metadata().index(indexName).getVersion());

            DocTableInfo newTable = new DocTableInfoFactory(e.nodeCtx).create(tbl.ident(), newState.metadata());

            assertThat(newTable.getReference(colToDrop.column())).isNull();
            assertThat(newTable.columns()).hasSize(2);
            assertThat(newTable.droppedColumns()).satisfiesExactly(
                x -> assertThat(x).isReference()
                    .hasName("_dropped_2")
                    .hasPosition(2)
            );
        }
    }

    @Test
    public void test_can_drop_subcolumn() throws Exception {
        var e = SQLExecutor.of(clusterService)
            .addTable("create table tbl (id int, o object AS(a int, b int, oo object AS (a int, b int)))");
        DocTableInfo tbl = e.resolveTableInfo("tbl");
        try (IndexEnv indexEnv = new IndexEnv(
            THREAD_POOL,
            tbl,
            clusterService.state(),
            Version.CURRENT
        )) {
            var dropColumnTask = new AlterTableTask<>(
                e.nodeCtx, imd -> indexEnv.mapperService(), tbl.ident(), TransportDropColumnAction.DROP_COLUMN_OPERATOR);
            Reference colToDrop = tbl.getReference(new ColumnIdent("o", "oo"));
            var request = new DropColumnRequest(tbl.ident(), List.of(new DropColumn(colToDrop, false)));
            ClusterState newState = dropColumnTask.execute(clusterService.state(), request);
            DocTableInfo newTable = new DocTableInfoFactory(e.nodeCtx).create(tbl.ident(), newState.metadata());

            assertThat(newTable.getReference(colToDrop.column())).isNull();
            Reference o = newTable.getReference(new ColumnIdent("o"));
            assertThat(o.valueType()).isExactlyInstanceOf(ObjectType.class);
            ObjectType objectType = (ObjectType) o.valueType();
            assertThat(objectType.innerTypes())
                .as("Parent column objectType is updated")
                .containsExactly(
                    Map.entry("a", DataTypes.INTEGER),
                    Map.entry("b", DataTypes.INTEGER)
                );
        }
    }

    @Test
    public void test_can_drop_subcolumn_and_parent_together() throws Exception {
        var e = SQLExecutor.of(clusterService)
            .addTable("create table tbl (o object as (o2 object as (c int)))");
        DocTableInfo tbl = e.resolveTableInfo("tbl");
        try (IndexEnv indexEnv = new IndexEnv(
            THREAD_POOL,
            tbl,
            clusterService.state(),
            Version.CURRENT
        )) {
            var dropColumnTask = new DropColumnTask(e.nodeCtx, imd -> indexEnv.mapperService());
            // parent specified first then its child
            Reference colToDrop1 = tbl.getReference(new ColumnIdent("o", "o2"));
            Reference colToDrop2 = tbl.getReference(new ColumnIdent("o", List.of("o2", "c")));
            var request = new DropColumnRequest(tbl.ident(), List.of(
                new DropColumn(colToDrop1, false),
                new DropColumn(colToDrop2, false)));
            ClusterState newState = dropColumnTask.execute(clusterService.state(), request);
            DocTableInfo newTable = new DocTableInfoFactory(e.nodeCtx).create(tbl.ident(), newState.metadata());

            assertThat(newTable.getReference(colToDrop1.column())).isNull();
            assertThat(newTable.getReference(colToDrop2.column())).isNull();
        }
    }

    @Test
    public void test_is_no_op_if_columns_exist() throws Exception {
        var e = SQLExecutor.of(clusterService)
            .addTable("create table tbl (x int, y int)");
        DocTableInfo tbl = e.resolveTableInfo("tbl");
        ClusterState state = clusterService.state();
        try (IndexEnv indexEnv = new IndexEnv(
            THREAD_POOL,
            tbl,
            state,
            Version.CURRENT
        )) {
            var dropColumnTask = new AlterTableTask<>(
                e.nodeCtx, imd -> indexEnv.mapperService(), tbl.ident(), TransportDropColumnAction.DROP_COLUMN_OPERATOR);
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

    @Test
    public void test_drop_column_with_check_constraint() throws Exception {
        var e = SQLExecutor.of(clusterService)
            .addTable("create table tbl (x int check (x > 0), y int check (y > 0))");
        DocTableInfo tbl = e.resolveTableInfo("tbl");
        ClusterState state = clusterService.state();
        try (IndexEnv indexEnv = new IndexEnv(
            THREAD_POOL,
            tbl,
            state,
            Version.CURRENT
        )) {
            var dropColumnTask = new AlterTableTask<>(
                e.nodeCtx, imd -> indexEnv.mapperService(), tbl.ident(), TransportDropColumnAction.DROP_COLUMN_OPERATOR);
            ReferenceIdent refIdent = new ReferenceIdent(tbl.ident(), "y");
            SimpleReference colToDrop = new SimpleReference(
                refIdent,
                RowGranularity.DOC,
                DataTypes.INTEGER,
                333, // irrelevant
                null
            );
            var request = new DropColumnRequest(tbl.ident(), List.of(new DropColumn(colToDrop, false)));
            ClusterState newState = dropColumnTask.execute(clusterService.state(), request);
            DocTableInfo newTable = new DocTableInfoFactory(e.nodeCtx).create(tbl.ident(), newState.metadata());

            assertThat(newTable.getReference(colToDrop.column())).isNull();
            assertThat(newTable.checkConstraints()).hasSize(1);
            assertThat(newTable.checkConstraints().get(0).expressionStr()).isEqualTo("\"x\" > 0");
        }
    }

    @Test
    public void test_cannot_drop_column_used_in_generated_expression() throws Exception {
        var e = SQLExecutor.of(clusterService)
            .addTable("create table tbl (x int, y int, z as (y + 1))");
        DocTableInfo tbl = e.resolveTableInfo("tbl");
        ClusterState state = clusterService.state();
        try (IndexEnv indexEnv = new IndexEnv(
            THREAD_POOL,
            tbl,
            state,
            Version.CURRENT
        )) {
            var dropColumnTask = new AlterTableTask<>(
                e.nodeCtx, imd -> indexEnv.mapperService(), tbl.ident(), TransportDropColumnAction.DROP_COLUMN_OPERATOR);
            Reference ref = tbl.getReference(new ColumnIdent("y"));

            DropColumnRequest request = new DropColumnRequest(tbl.ident(), List.of(new DropColumn(ref, true)));
            assertThatThrownBy(() -> dropColumnTask.execute(state, request))
                .isExactlyInstanceOf(UnsupportedOperationException.class)
                .hasMessage(
                    "Cannot drop column `y`. It's used in generated column `z`: (y + 1)");
        }
    }

    @Test
    public void test_drop_subcolumn_with_check_constraint() throws Exception {
        var e = SQLExecutor.of(clusterService)
            .addTable("create table tbl (id int, o object AS(a int, b int, oo object AS (" +
                      "a int check (o['oo']['a'] > 0), b int)))");
        DocTableInfo tbl = e.resolveTableInfo("tbl");
        try (IndexEnv indexEnv = new IndexEnv(
            THREAD_POOL,
            tbl,
            clusterService.state(),
            Version.CURRENT
        )) {
            var dropColumnTask = new AlterTableTask<>(
                e.nodeCtx, imd -> indexEnv.mapperService(), tbl.ident(), TransportDropColumnAction.DROP_COLUMN_OPERATOR);
            ReferenceIdent refIdent = new ReferenceIdent(tbl.ident(), "o", List.of("oo", "a"));
            SimpleReference colToDrop = new SimpleReference(
                refIdent,
                RowGranularity.DOC,
                DataTypes.SHORT, // irrelevant
                333, // irrelevant
                null
            );
            var request = new DropColumnRequest(tbl.ident(), List.of(new DropColumn(colToDrop, false)));
            ClusterState newState = dropColumnTask.execute(clusterService.state(), request);
            DocTableInfo newTable = new DocTableInfoFactory(e.nodeCtx).create(tbl.ident(), newState.metadata());

            assertThat(newTable.getReference(colToDrop.column())).isNull();
            assertThat(newTable.checkConstraints()).isEmpty();
        }
    }

    @Test
    public void test_drop_column_with_table_level_check_constraint() throws Exception {
        var e = SQLExecutor.of(clusterService)
            .addTable("create table tbl (x int, y int, check (x > 0), check (y > 0))");
        DocTableInfo tbl = e.resolveTableInfo("tbl");
        ClusterState state = clusterService.state();
        try (IndexEnv indexEnv = new IndexEnv(
            THREAD_POOL,
            tbl,
            state,
            Version.CURRENT
        )) {
            var dropColumnTask = new AlterTableTask<>(
                e.nodeCtx, imd -> indexEnv.mapperService(), tbl.ident(), TransportDropColumnAction.DROP_COLUMN_OPERATOR);
            ReferenceIdent refIdent = new ReferenceIdent(tbl.ident(), "y");
            SimpleReference colToDrop = new SimpleReference(
                refIdent,
                RowGranularity.DOC,
                DataTypes.INTEGER,
                333, // irrelevant
                null
            );
            var request = new DropColumnRequest(tbl.ident(), List.of(new DropColumn(colToDrop, false)));
            ClusterState newState = dropColumnTask.execute(clusterService.state(), request);
            DocTableInfo newTable = new DocTableInfoFactory(e.nodeCtx).create(tbl.ident(), newState.metadata());

            assertThat(newTable.getReference(colToDrop.column())).isNull();
            assertThat(newTable.checkConstraints()).hasSize(1);
            assertThat(newTable.checkConstraints().get(0).expressionStr()).isEqualTo("\"x\" > 0");
        }
    }

    @Test
    public void test_drop_column_with_check_constraint_from_partitioned_table() throws Exception {
        var e = SQLExecutor.of(clusterService)
            .addPartitionedTable("create table doc.parted(x int check (x > 0), y int check (y > 0)) " +
                                 "partitioned by (x)" ,
                new PartitionName(new RelationName("doc", "parted"), singletonList("1")).asIndexName(),
                new PartitionName(new RelationName("doc", "parted"), singletonList("2")).asIndexName(),
                new PartitionName(new RelationName("doc", "parted"), singletonList(null)).asIndexName());
        DocTableInfo tbl = e.resolveTableInfo("doc.parted");
        var dropColumnTask = new AlterTableTask<>(e.nodeCtx, imd -> MapperTestUtils.newMapperService(
            new NamedXContentRegistry(ClusterModule.getNamedXWriteables()),
            createTempDir(),
            Settings.EMPTY,
            "doc.parted"),
            tbl.ident(),
            TransportDropColumnAction.DROP_COLUMN_OPERATOR);
        ReferenceIdent refIdent = new ReferenceIdent(tbl.ident(), "y");
        SimpleReference colToDrop = new SimpleReference(
            refIdent,
            RowGranularity.DOC,
            DataTypes.INTEGER,
            333, // irrelevant
            null
        );
        var request = new DropColumnRequest(tbl.ident(), List.of(new DropColumn(colToDrop, false)));
        ClusterState newState = dropColumnTask.execute(clusterService.state(), request);
        DocTableInfo newTable = new DocTableInfoFactory(e.nodeCtx).create(tbl.ident(), newState.metadata());

        assertThat(newTable.getReference(colToDrop.column())).isNull();
        assertThat(newTable.checkConstraints()).hasSize(1);
        assertThat(newTable.checkConstraints().get(0).expressionStr()).isEqualTo("\"x\" > 0");
    }

    @Test
    public void test_drop_subcolumn_with_check_constraint_on_children() throws Exception {
        var e = SQLExecutor.of(clusterService)
            .addTable("create table tbl (id int, o object AS(a int, b int, oo object AS (" +
                "a int, ooo object AS (a int))), check (o['oo']['a'] + o['oo']['ooo']['a']> 0))");
        DocTableInfo tbl = e.resolveTableInfo("tbl");
        try (IndexEnv indexEnv = new IndexEnv(
            THREAD_POOL,
            tbl,
            clusterService.state(),
            Version.CURRENT
        )) {
            var dropColumnTask = new AlterTableTask<>(
                e.nodeCtx, imd -> indexEnv.mapperService(), tbl.ident(), TransportDropColumnAction.DROP_COLUMN_OPERATOR);
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
            DocTableInfo newTable = new DocTableInfoFactory(e.nodeCtx).create(tbl.ident(), newState.metadata());

            assertThat(newTable.getReference(colToDrop.column())).isNull();
            assertThat(newTable.checkConstraints()).isEmpty();
        }
    }
}
