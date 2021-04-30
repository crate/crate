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

import io.crate.common.collections.Maps;
import io.crate.metadata.CoordinatorTxnCtx;
import io.crate.metadata.TransactionContext;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.SQLExecutor;
import org.junit.Test;

import java.util.Map;

import static org.hamcrest.Matchers.is;

public class GeneratedColsFromRawInsertSourceTest extends CrateDummyClusterServiceUnitTest {

    private TransactionContext txnCtx = CoordinatorTxnCtx.systemTransactionContext();

    @Test
    public void test_generated_based_on_default() throws Exception {
        var e = SQLExecutor.builder(clusterService)
            .addTable("create table generated_based_on_default (x int default 1, y as x + 1)")
            .build();
        DocTableInfo t = e.resolveTableInfo("generated_based_on_default");
        GeneratedColsFromRawInsertSource insertSource = new GeneratedColsFromRawInsertSource(
            txnCtx, e.nodeCtx, t.generatedColumns(), t.defaultExpressionColumns());
        Map<String, Object> map = insertSource.generateSourceAndCheckConstraints(new Object[]{"{}"});
        assertThat(Maps.getByPath(map, "x"), is(1));
        assertThat(Maps.getByPath(map, "y"), is(2));
    }

    @Test
    public void test_value_is_not_overwritten_by_default() throws Exception {
        var e = SQLExecutor.builder(clusterService)
            .addTable("create table generated_based_on_default (x int default 1, y as x + 1)")
            .build();
        DocTableInfo t = e.resolveTableInfo("generated_based_on_default");
        GeneratedColsFromRawInsertSource insertSource = new GeneratedColsFromRawInsertSource(
            txnCtx, e.nodeCtx, t.generatedColumns(), t.defaultExpressionColumns());
        Map<String, Object> map = insertSource.generateSourceAndCheckConstraints(new Object[]{"{\"x\":2}"});
        assertThat(Maps.getByPath(map, "x"), is(2));
        assertThat(Maps.getByPath(map, "y"), is(3));
    }

    @Test
    public void test_generate_value_text_type_with_length_exceeding_whitespaces_trimmed() throws Exception {
        var e = SQLExecutor.builder(clusterService)
            .addTable("create table t (x varchar(2) as 'ab  ')")
            .build();
        DocTableInfo t = e.resolveTableInfo("t");
        var insertSource = new GeneratedColsFromRawInsertSource(
            txnCtx, e.nodeCtx, t.generatedColumns(), t.defaultExpressionColumns());
        Map<String, Object> map = insertSource.generateSourceAndCheckConstraints(new Object[]{"{}"});
        assertThat(Maps.getByPath(map, "x"), is("ab"));
    }

    @Test
    public void test_generate_value_that_exceeds_text_type_with_length_throws_exception() throws Exception {
        var e = SQLExecutor.builder(clusterService)
            .addTable("create table t (x varchar(2) as 'abc')")
            .build();
        DocTableInfo t = e.resolveTableInfo("t");
        var insertSource = new GeneratedColsFromRawInsertSource(
            txnCtx, e.nodeCtx, t.generatedColumns(), t.defaultExpressionColumns());
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("'abc' is too long for the text type of length: 2");
        insertSource.generateSourceAndCheckConstraints(new Object[]{"{}"});
    }

    @Test
    public void test_default_value_that_exceeds_text_type_with_length_throws_exception() throws Exception {
        var e = SQLExecutor.builder(clusterService)
            .addTable("create table t (x varchar(2) as 'abc')")
            .build();
        DocTableInfo t = e.resolveTableInfo("t");
        var insertSource = new GeneratedColsFromRawInsertSource(
            txnCtx, e.nodeCtx, t.generatedColumns(), t.defaultExpressionColumns());
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("'abc' is too long for the text type of length: 2");
        insertSource.generateSourceAndCheckConstraints(new Object[]{"{}"});
    }
}
