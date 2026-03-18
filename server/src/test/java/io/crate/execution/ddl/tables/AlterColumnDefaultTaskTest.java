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
import static org.assertj.core.api.Assertions.assertThat;

import org.elasticsearch.cluster.ClusterState;
import org.junit.Test;

import io.crate.expression.symbol.Literal;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.Reference;
import io.crate.metadata.RelationName;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.metadata.doc.DocTableInfoFactory;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.SQLExecutor;

public class AlterColumnDefaultTaskTest extends CrateDummyClusterServiceUnitTest {

    private static AlterTableTask<AlterColumnDefaultRequest> buildAlterColumnDefaultTask(SQLExecutor e, RelationName tblName) {
        return new AlterTableTask<>(
            e.nodeCtx, tblName, e.fulltextAnalyzerResolver(), TransportAlterColumnDefault.ALTER_COLUMN_DEFAULT_OPERATOR);
    }

    @Test
    public void test_set_default_on_column_without_default() throws Exception {
        var e = SQLExecutor.of(clusterService)
            .addTable("create table tbl (x int)");
        DocTableInfo tbl = e.resolveTableInfo("tbl");
        var task = buildAlterColumnDefaultTask(e, tbl.ident());
        Reference ref = tbl.getReference(ColumnIdent.of("x"));
        Symbol newDefault = Literal.of(42);
        var request = new AlterColumnDefaultRequest(tbl.ident(), ref, newDefault);
        ClusterState newState = task.execute(clusterService.state(), request);
        DocTableInfo newTable = new DocTableInfoFactory(e.nodeCtx).create(tbl.ident(), newState.metadata());
        Reference updatedRef = newTable.getReference(ColumnIdent.of("x"));
        assertThat(updatedRef.defaultExpression()).isEqualTo(Literal.of(42));
    }

    @Test
    public void test_change_existing_default() throws Exception {
        var e = SQLExecutor.of(clusterService)
            .addTable("create table tbl (x int default 1)");
        DocTableInfo tbl = e.resolveTableInfo("tbl");
        var task = buildAlterColumnDefaultTask(e, tbl.ident());
        Reference ref = tbl.getReference(ColumnIdent.of("x"));
        assertThat(ref.defaultExpression()).isEqualTo(Literal.of(1));
        Symbol newDefault = Literal.of(99);
        var request = new AlterColumnDefaultRequest(tbl.ident(), ref, newDefault);
        ClusterState newState = task.execute(clusterService.state(), request);
        DocTableInfo newTable = new DocTableInfoFactory(e.nodeCtx).create(tbl.ident(), newState.metadata());
        Reference updatedRef = newTable.getReference(ColumnIdent.of("x"));
        assertThat(updatedRef.defaultExpression()).isEqualTo(Literal.of(99));
    }

    @Test
    public void test_drop_default() throws Exception {
        var e = SQLExecutor.of(clusterService)
            .addTable("create table tbl (x int default 42)");
        DocTableInfo tbl = e.resolveTableInfo("tbl");
        var task = buildAlterColumnDefaultTask(e, tbl.ident());
        Reference ref = tbl.getReference(ColumnIdent.of("x"));
        assertThat(ref.defaultExpression()).isNotNull();
        var request = new AlterColumnDefaultRequest(tbl.ident(), ref, null);
        ClusterState newState = task.execute(clusterService.state(), request);
        DocTableInfo newTable = new DocTableInfoFactory(e.nodeCtx).create(tbl.ident(), newState.metadata());
        Reference updatedRef = newTable.getReference(ColumnIdent.of("x"));
        assertThat(updatedRef.defaultExpression()).isNull();
    }

    @Test
    public void test_drop_default_on_column_without_default_is_noop() throws Exception {
        var e = SQLExecutor.of(clusterService)
            .addTable("create table tbl (x int)");
        DocTableInfo tbl = e.resolveTableInfo("tbl");
        var task = buildAlterColumnDefaultTask(e, tbl.ident());
        Reference ref = tbl.getReference(ColumnIdent.of("x"));
        var request = new AlterColumnDefaultRequest(tbl.ident(), ref, null);
        ClusterState newState = task.execute(clusterService.state(), request);
        // No-op: cluster state should be the same object
        assertThat(newState).isSameAs(clusterService.state());
    }

    @Test
    public void test_set_default_on_nested_column() throws Exception {
        var e = SQLExecutor.of(clusterService)
            .addTable("create table tbl (o object as (a int))");
        DocTableInfo tbl = e.resolveTableInfo("tbl");
        var task = buildAlterColumnDefaultTask(e, tbl.ident());
        Reference ref = tbl.getReference(ColumnIdent.of("o", "a"));
        Symbol newDefault = Literal.of(10);
        var request = new AlterColumnDefaultRequest(tbl.ident(), ref, newDefault);
        ClusterState newState = task.execute(clusterService.state(), request);
        DocTableInfo newTable = new DocTableInfoFactory(e.nodeCtx).create(tbl.ident(), newState.metadata());
        Reference updatedRef = newTable.getReference(ColumnIdent.of("o", "a"));
        assertThat(updatedRef.defaultExpression()).isEqualTo(Literal.of(10));
    }

    @Test
    public void test_set_default_on_partitioned_table() throws Exception {
        var e = SQLExecutor.of(clusterService)
            .addTable("create table tbl (x int, p text) partitioned by (p)",
                java.util.List.of("p1"),
                java.util.List.of("p2")
            );
        DocTableInfo tbl = e.resolveTableInfo("tbl");
        var task = buildAlterColumnDefaultTask(e, tbl.ident());
        Reference ref = tbl.getReference(ColumnIdent.of("x"));
        Symbol newDefault = Literal.of(77);
        var request = new AlterColumnDefaultRequest(tbl.ident(), ref, newDefault);
        ClusterState newState = task.execute(clusterService.state(), request);
        DocTableInfo newTable = new DocTableInfoFactory(e.nodeCtx).create(tbl.ident(), newState.metadata());
        Reference updatedRef = newTable.getReference(ColumnIdent.of("x"));
        assertThat(updatedRef.defaultExpression()).isEqualTo(Literal.of(77));
    }
}
