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

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

import java.io.IOException;
import java.util.Map;

import org.junit.Test;

import io.crate.analyze.AnalyzedUpdateStatement;
import io.crate.expression.reference.Doc;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.CoordinatorTxnCtx;
import io.crate.metadata.TransactionContext;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.SQLExecutor;

public class ReturnValueGenTest extends CrateDummyClusterServiceUnitTest {

    private final String CREATE_TEST_TABLE = "create table test (id int primary key, message string)";

    @Test
    public void test_update_returning_id() throws Exception {
        setupStatement("update test set message='updated' where id = 1 returning _docid");
        Object[] objects = returnValues("1", Map.of());
        assertThat(objects[0], is(1));
    }

    @Test
    public void test_update_by_id_returning_multiple_fields() throws Exception {
        setupStatement("update test set message='updated' where id = 1 returning _docid, message");
        Object[] objects = returnValues("1", Map.of("message", "updated"));
        assertThat(objects[0], is(1));
        assertThat(objects[1], is("updated"));
    }

    @Test
    public void test_update_returning_with_system_columns() throws Exception {
        setupStatement("update test set message='update' returning _seq_no");
        Object[] objects = returnValues("1", Map.of("message", "updated"));
        assertThat(objects[0], is(1L));
    }

    @Test
    public void test_update_by_id_returning_functions() throws Exception {
        setupStatement("update test set message='updated' where id = 1 returning _docid + 1::int, UPPER(message)");
        Object[] objects = returnValues("1", Map.of("message", "updated"));
        assertThat(objects[0], is(2));
        assertThat(objects[1], is("UPDATED"));
    }

    @Test
    public void test_update_by_id_returning_functions_multiple_inputs() throws Exception {
        setupStatement("update test set message='updated' where id = 1 returning _docid + _seq_no + 1");
        Object[] objects = returnValues("1", Map.of("message", "updated"));
        assertThat(objects[0], is(3L));
    }

    private Object[] returnValues(String id, Map<String, Object> content) {
        return returnValueGen.generateReturnValues(doc(id, content));
    }

    private void setupStatement(String stmt) throws IOException {
        SQLExecutor executor = SQLExecutor.builder(clusterService).addTable(CREATE_TEST_TABLE).build();
        AnalyzedUpdateStatement update = executor.analyze(stmt);
        tableInfo = (DocTableInfo) update.table().tableInfo();
        returnValueGen = new ReturnValueGen(txnCtx,
                                            executor.nodeCtx,
                                            tableInfo,
                                            update.outputs() == null ? null : update.outputs().toArray(new Symbol[0]));
    }

    private Doc doc(String id, Map<String, Object> content) {
        return new Doc(1, tableInfo.concreteIndices()[0], id, 1, 1, 1, content, () -> "");
    }

    private TransactionContext txnCtx = CoordinatorTxnCtx.systemTransactionContext();
    private DocTableInfo tableInfo;
    private ReturnValueGen returnValueGen;
}

