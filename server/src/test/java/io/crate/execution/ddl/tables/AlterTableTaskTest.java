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

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;

import org.elasticsearch.cluster.ClusterState;
import org.junit.Test;

import io.crate.analyze.DropColumn;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.Reference;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.metadata.doc.DocTableInfoFactory;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.SQLExecutor;

public class AlterTableTaskTest extends CrateDummyClusterServiceUnitTest {

    @Test
    public void test_uses_table_oid_to_resolve_table() throws Exception {
        var e = SQLExecutor.of(clusterService)
            .addTable("create table t1 (x int, y int)")
            .addTable("create table t2 (x int, y int)");
        DocTableInfo t1 = e.resolveTableInfo("t1");
        DocTableInfo t2 = e.resolveTableInfo("t2");
        var dropColumnTask = new AlterTableTask<>(
            e.nodeCtx,
            // t2's name and t1's oid
            t2.ident(), t1.oid(),
            e.fulltextAnalyzerResolver(),
            TransportDropColumn.DROP_COLUMN_OPERATOR
        );
        Reference colToDrop = t1.getReference(ColumnIdent.of("y"));
        var request = new DropColumnRequest(
            // t2's name and t1's oid
            t2.ident(), t1.oid(),
            List.of(new DropColumn(colToDrop, false)));

        ClusterState newState = dropColumnTask.execute(clusterService.state(), request);
        var docTableInfoFactory = new DocTableInfoFactory(e.nodeCtx);
        DocTableInfo newT1 = docTableInfoFactory.create(t1.ident(), newState.metadata());
        DocTableInfo newT2 = docTableInfoFactory.create(t2.ident(), newState.metadata());

        // 'y' is dropped from t1 but not t2, which means the table was resolved by the oid
        assertThat(newT1.getReference(ColumnIdent.of("y"))).isNull();
        assertThat(newT2.getReference(ColumnIdent.of("y"))).isNotNull();
    }
}
