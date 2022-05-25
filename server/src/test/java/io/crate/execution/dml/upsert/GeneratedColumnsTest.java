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

import io.crate.analyze.QueriedSelectRelation;
import io.crate.analyze.relations.DocTableRelation;
import io.crate.data.Input;
import io.crate.expression.InputFactory;
import io.crate.expression.reference.Doc;
import io.crate.expression.reference.DocRefResolver;
import io.crate.metadata.CoordinatorTxnCtx;
import io.crate.metadata.SimpleReference;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.SQLExecutor;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentType;
import org.junit.Test;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.contains;

public class GeneratedColumnsTest extends CrateDummyClusterServiceUnitTest {

    @Test
    public void testSubscriptExpressionThatReturnsAnArray() throws Exception {
        SQLExecutor e = SQLExecutor.builder(clusterService)
            .addTable("create table t (obj object as (arr array(integer)), arr as obj['arr'])")
            .build();
        QueriedSelectRelation query = e.analyze("select obj, arr from t");
        DocTableInfo table = ((DocTableRelation) query.from().get(0)).tableInfo();
        GeneratedColumns<Doc> generatedColumns = new GeneratedColumns<>(
            new InputFactory(e.nodeCtx),
            CoordinatorTxnCtx.systemTransactionContext(),
            false,
            new DocRefResolver(Collections.emptyList()),
            Collections.emptyList(),
            table.generatedColumns()
        );

        BytesReference bytes = BytesReference.bytes(XContentFactory.jsonBuilder()
            .startObject()
            .startObject("obj")
                .startArray("arr")
                    .value(10)
                    .value(20)
                .endArray()
            .endObject()
            .endObject());
        generatedColumns.setNextRow(new Doc(
            1,
            table.concreteIndices()[0],
            "1",
            1,
            1,
            1,
            XContentHelper.convertToMap(bytes, false, XContentType.JSON).v2(),
            bytes::utf8ToString
        ));
        Map.Entry<SimpleReference, Input<?>> generatedColumn = generatedColumns.generatedToInject().iterator().next();
        assertThat((List<Object>) generatedColumn.getValue().value(), contains(10, 20));
    }
}
