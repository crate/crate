/*
 * Licensed to Crate under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial
 * agreement.
 */

package io.crate.execution.dml.upsert;

import io.crate.Constants;
import io.crate.analyze.AnalyzedUpdateStatement;
import io.crate.expression.reference.Doc;
import io.crate.expression.symbol.Assignments;
import io.crate.metadata.PartitionName;
import io.crate.metadata.RelationName;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.SQLExecutor;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.get.GetResult;
import org.junit.Test;

import java.util.Collections;

import static java.util.Collections.emptyMap;
import static org.hamcrest.Matchers.is;

public class UpdateSourceGenTest extends CrateDummyClusterServiceUnitTest {

    @Test
    public void testSetXBasedOnXAndPartitionedColumn() throws Exception {
        SQLExecutor e = SQLExecutor.builder(clusterService)
            .addPartitionedTable("create table t (x int, p int) partitioned by (p)",
                new PartitionName(new RelationName("doc", "t"), Collections.singletonList("1")).asIndexName())
            .build();

        AnalyzedUpdateStatement update = e.analyze("update t set x = x + p");
        Assignments assignments = Assignments.convert(update.assignmentByTargetCol());
        DocTableInfo table = (DocTableInfo) update.table().tableInfo();
        UpdateSourceGen updateSourceGen = new UpdateSourceGen(
            e.functions(),
            table,
            assignments.targetNames()
        );

        BytesReference source = BytesReference.bytes(XContentFactory.jsonBuilder()
            .startObject()
            .field("x", 1)
            .endObject());
        BytesReference updatedSource = updateSourceGen.generateSource(
            Doc.fromGetResult(new GetResult(
                table.concreteIndices()[0],
                Constants.DEFAULT_MAPPING_TYPE,
                "1",
                1,
                true,
                source,
                emptyMap())
            ),
            assignments.sources(),
            new Object[0]
        );
        assertThat(updatedSource.utf8ToString(), is("{\"x\":2}"));
    }

    @Test
    public void testSourceGenerationWithAssignmentUsingDocumentPrimaryKey() throws Exception {
        SQLExecutor e = SQLExecutor.builder(clusterService)
            .addTable("create table t (y int)")
            .build();
        AnalyzedUpdateStatement update = e.analyze("update t set y = _id::integer * 2");
        Assignments assignments = Assignments.convert(update.assignmentByTargetCol());
        DocTableInfo table = (DocTableInfo) update.table().tableInfo();
        UpdateSourceGen updateSourceGen = new UpdateSourceGen(
            e.functions(),
            table,
            assignments.targetNames()
        );

        BytesReference source = BytesReference.bytes(XContentFactory.jsonBuilder()
            .startObject()
            .field("y", 100)
            .endObject());
        BytesReference updatedSource = updateSourceGen.generateSource(
            Doc.fromGetResult(new GetResult(
                table.concreteIndices()[0],
                Constants.DEFAULT_MAPPING_TYPE,
                "4",
                1,
                true,
                source,
                emptyMap())
            ),
            assignments.sources(),
            new Object[0]
        );
        assertThat(updatedSource.utf8ToString(), is("{\"y\":8}"));
    }

    @Test
    public void testNestedGeneratedColumnIsGenerated() throws Exception {
        SQLExecutor e = SQLExecutor.builder(clusterService)
            .addTable("create table t (x int, obj object as (y as x + 1))")
            .build();
        AnalyzedUpdateStatement update = e.analyze("update t set x = 4");
        Assignments assignments = Assignments.convert(update.assignmentByTargetCol());
        DocTableInfo table = (DocTableInfo) update.table().tableInfo();
        UpdateSourceGen updateSourceGen = new UpdateSourceGen(
            e.functions(),
            table,
            assignments.targetNames()
        );
        BytesReference updatedSource = updateSourceGen.generateSource(
            Doc.fromGetResult(new GetResult(
                table.concreteIndices()[0],
                Constants.DEFAULT_MAPPING_TYPE,
                "1",
                1,
                true,
                new BytesArray("{}"),
                emptyMap()
            )),
            assignments.sources(),
            new Object[0]
        );
        assertThat(updatedSource.utf8ToString(), is("{\"obj\":{\"y\":5},\"x\":4}"));
    }
}
