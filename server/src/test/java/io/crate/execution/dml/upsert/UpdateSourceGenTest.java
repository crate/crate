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

package io.crate.execution.dml.upsert;

import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonMap;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.junit.Test;

import io.crate.analyze.AnalyzedUpdateStatement;
import io.crate.expression.reference.Doc;
import io.crate.expression.symbol.Assignments;
import io.crate.metadata.CoordinatorTxnCtx;
import io.crate.metadata.PartitionName;
import io.crate.metadata.RelationName;
import io.crate.metadata.SearchPath;
import io.crate.metadata.TransactionContext;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.metadata.settings.SessionSettings;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.SQLExecutor;

public class UpdateSourceGenTest extends CrateDummyClusterServiceUnitTest {

    private static SessionSettings DUMMY_SESSION_INFO = new SessionSettings(
        "dummyUser",
        SearchPath.createSearchPathFrom("dummySchema"));

    private TransactionContext txnCtx = CoordinatorTxnCtx.systemTransactionContext();

    @Test
    public void testSetXBasedOnXAndPartitionedColumn() throws Exception {
        SQLExecutor e = SQLExecutor.builder(clusterService)
            .addPartitionedTable("create table t (x int, p int) partitioned by (p)",
                new PartitionName(new RelationName("doc", "t"), Collections.singletonList("1")).asIndexName())
            .build();

        AnalyzedUpdateStatement update = e.analyze("update t set x = x + p");
        Assignments assignments = Assignments.convert(update.assignmentByTargetCol(), e.nodeCtx);
        DocTableInfo table = (DocTableInfo) update.table().tableInfo();
        UpdateSourceGen updateSourceGen = new UpdateSourceGen(
            txnCtx,
            e.nodeCtx,
            table,
            assignments.targetNames()
        );

        Map<String, Object> source = singletonMap("x", 1);
        Map<String, Object> updatedSource = updateSourceGen.generateSource(
            new Doc(
                1,
                table.concreteIndices()[0],
                "1",
                1,
                1,
                1,
                source,
                () -> {
                    try {
                        return Strings.toString(XContentFactory.jsonBuilder().map(source));
                    } catch (IOException e1) {
                        throw new RuntimeException(e1);
                    }
                }
            ),
            assignments.sources(),
            new Object[0]
        );
        assertThat(updatedSource, is(Map.of("x", 2)));
    }

    @Test
    public void testSourceGenerationWithAssignmentUsingDocumentPrimaryKey() throws Exception {
        SQLExecutor e = SQLExecutor.builder(clusterService)
            .addTable("create table t (y int)")
            .build();
        AnalyzedUpdateStatement update = e.analyze("update t set y = _id::integer * 2");
        Assignments assignments = Assignments.convert(update.assignmentByTargetCol(), e.nodeCtx);
        DocTableInfo table = (DocTableInfo) update.table().tableInfo();
        UpdateSourceGen updateSourceGen = new UpdateSourceGen(
            txnCtx,
            e.nodeCtx,
            table,
            assignments.targetNames()
        );

        BytesReference source = BytesReference.bytes(XContentFactory.jsonBuilder()
            .startObject()
            .field("y", 100)
            .endObject());
        Map<String, Object> updatedSource = updateSourceGen.generateSource(
            new Doc(
                1,
                table.concreteIndices()[0],
                "4",
                1,
                1,
                1,
                emptyMap(),
                source::utf8ToString
            ),
            assignments.sources(),
            new Object[0]
        );
        assertThat(updatedSource, is(Map.of("y", 8)));
    }

    @Test
    public void testNestedGeneratedColumnIsGenerated() throws Exception {
        SQLExecutor e = SQLExecutor.builder(clusterService)
            .addTable("create table t (x int, obj object as (y as x + 1))")
            .build();
        AnalyzedUpdateStatement update = e.analyze("update t set x = 4");
        Assignments assignments = Assignments.convert(update.assignmentByTargetCol(), e.nodeCtx);
        DocTableInfo table = (DocTableInfo) update.table().tableInfo();
        UpdateSourceGen updateSourceGen = new UpdateSourceGen(
            txnCtx,
            e.nodeCtx,
            table,
            assignments.targetNames()
        );
        Map<String, Object> updatedSource = updateSourceGen.generateSource(
            new Doc(
                1,
                table.concreteIndices()[0],
                "1",
                1,
                1,
                1,
                emptyMap(),
                () -> "{}"
            ),
            assignments.sources(),
            new Object[0]
        );
        assertThat(updatedSource, is(Map.of("obj", Map.of("y", 5), "x", 4)));
    }


    @Test
    public void testGeneratedColumnUsingFunctionDependingOnActiveTransaction() throws Exception {
        SQLExecutor e = SQLExecutor.builder(clusterService)
            .addTable("create table t (x int, gen as current_schema)")
            .build();
        AnalyzedUpdateStatement update = e.analyze("update t set x = 1");
        Assignments assignments = Assignments.convert(update.assignmentByTargetCol(), e.nodeCtx);
        DocTableInfo table = (DocTableInfo) update.table().tableInfo();
        UpdateSourceGen sourceGen = new UpdateSourceGen(
            TransactionContext.of(DUMMY_SESSION_INFO),
            e.nodeCtx,
            table,
            assignments.targetNames()
        );

        Map<String, Object> source = sourceGen.generateSource(
            new Doc(1, table.concreteIndices()[0], "1", 1, 1, 1, emptyMap(), () -> "{}"),
            assignments.sources(),
            new Object[0]
        );

        assertThat(source, is(Map.of("gen","dummySchema","x", 1)));
    }

    @Test
    public void testNestedGeneratedColumnRaiseErrorIfGivenByUserDoesNotMatch() throws Exception {
        SQLExecutor e = SQLExecutor.builder(clusterService)
            .addTable("create table t (x int, obj object as (y as 'foo'))")
            .build();
        AnalyzedUpdateStatement update = e.analyze("update t set x = 4, obj = {y='bar'}");
        Assignments assignments = Assignments.convert(update.assignmentByTargetCol(), e.nodeCtx);
        DocTableInfo table = (DocTableInfo) update.table().tableInfo();
        UpdateSourceGen updateSourceGen = new UpdateSourceGen(
            txnCtx,
            e.nodeCtx,
            table,
            assignments.targetNames()
        );

        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("Given value bar for generated column obj['y'] does not match calculation 'foo' = foo");
        updateSourceGen.generateSource(
            new Doc(
                1,
                table.concreteIndices()[0],
                "1",
                1,
                1,
                1,
                emptyMap(),
                () -> "{}"
            ),
            assignments.sources(),
            new Object[0]
        );
    }

    @Test
    public void testNestedGeneratedColumnIsGeneratedValidateValueIfGivenByUser() throws Exception {
        SQLExecutor e = SQLExecutor.builder(clusterService)
            .addTable("create table t (x int, obj object as (y as 'foo'))")
            .build();
        AnalyzedUpdateStatement update = e.analyze("update t set x = 4, obj = {y='foo'}");
        Assignments assignments = Assignments.convert(update.assignmentByTargetCol(), e.nodeCtx);
        DocTableInfo table = (DocTableInfo) update.table().tableInfo();
        UpdateSourceGen updateSourceGen = new UpdateSourceGen(
            txnCtx,
            e.nodeCtx,
            table,
            assignments.targetNames()
        );
        Map<String, Object> updatedSource = updateSourceGen.generateSource(
            new Doc(
                1,
                table.concreteIndices()[0],
                "1",
                1,
                1,
                1,
                emptyMap(),
                () -> "{}"
            ),
            assignments.sources(),
            new Object[0]
        );
        assertThat(updatedSource, is(Map.of("obj", Map.of("y", "foo"), "x", 4)));
    }

    @Test
    public void test_update_child_of_object_column_that_is_null_implicitly_creates_the_object() throws Exception {
        SQLExecutor e = SQLExecutor.builder(clusterService)
            .addTable("create table t (obj object as (x int))")
            .build();
        AnalyzedUpdateStatement update = e.analyze("update t set obj['x'] = 10");
        Assignments assignments = Assignments.convert(update.assignmentByTargetCol(), e.nodeCtx);
        DocTableInfo table = (DocTableInfo) update.table().tableInfo();
        UpdateSourceGen updateSourceGen = new UpdateSourceGen(
            txnCtx,
            e.nodeCtx,
            table,
            assignments.targetNames()
        );
        Map<String, Object> updatedSource = updateSourceGen.generateSource(
            new Doc(
                1,
                table.concreteIndices()[0],
                "1",
                1,
                1,
                1,
                Collections.singletonMap("obj", null),
                () -> "{\"obj\": null}"
            ),
            assignments.sources(),
            new Object[0]
        );
        assertThat(updatedSource, is(Map.of("obj", Map.of("x", 10))));
    }
}
