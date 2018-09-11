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

import io.crate.analyze.QueriedTable;
import io.crate.analyze.relations.QueriedRelation;
import io.crate.metadata.Reference;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.SQLExecutor;
import org.elasticsearch.common.bytes.BytesReference;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;

import static java.util.Collections.singletonMap;
import static org.hamcrest.Matchers.is;

public class SourceFromCellsTest extends CrateDummyClusterServiceUnitTest {

    private SQLExecutor e;
    private DocTableInfo t1;
    private Reference x;
    private Reference y;
    private Reference z;
    private DocTableInfo t2;
    private Reference obj;
    private Reference b;

    @Before
    public void setUpExecutor() throws Exception {
        e = SQLExecutor.builder(clusterService)
            .addTable("create table t1 (x int, y int, z as x + y)")
            .addTable("create table t2 (obj object as (a int), b as obj['a'] + 1)")
            .build();
        QueriedRelation relation = e.analyze("select x, y, z from t1");
        t1 = (DocTableInfo) ((QueriedTable) relation).tableRelation().tableInfo();
        x = (Reference) relation.outputs().get(0);
        y = (Reference) relation.outputs().get(1);
        z = (Reference) relation.outputs().get(2);

        relation = e.analyze("select obj, b from t2");
        t2 = (DocTableInfo) ((QueriedTable) relation).tableRelation().tableInfo();
        obj = (Reference) relation.outputs().get(0);
        b = (Reference) relation.outputs().get(1);
    }

    @Test
    public void testGeneratedSourceBytesRef() throws IOException {
        InsertSourceFromCells sourceFromCells = new InsertSourceFromCells(
            e.functions(), t1, GeneratedColumns.Validation.VALUE_MATCH, Arrays.asList(x, y));
        BytesReference source = sourceFromCells.generateSource(new Object[]{1, 2});
        assertThat(source.utf8ToString(), is("{\"x\":1,\"y\":2,\"z\":3}"));
    }

    @Test
    public void testGenerateSourceRaisesAnErrorIfGeneratedColumnValueIsSuppliedByUserAndDoesNotMatch() throws IOException {
        InsertSourceFromCells sourceFromCells = new InsertSourceFromCells(
            e.functions(), t1, GeneratedColumns.Validation.VALUE_MATCH, Arrays.asList(x, y, z));

        expectedException.expectMessage("Given value 8 for generated column z does not match calculation (x + y) = 3");
        sourceFromCells.generateSource(new Object[]{1, 2, 8});
    }

    @Test
    public void testGeneratedColumnGenerationThatDependsOnNestedColumnOfObject() throws IOException {
        InsertSourceFromCells sourceFromCells = new InsertSourceFromCells(
            e.functions(), t2, GeneratedColumns.Validation.VALUE_MATCH, Collections.singletonList(obj));
        BytesReference source = sourceFromCells.generateSource(new Object[]{singletonMap("a", 10)});
        assertThat(source.utf8ToString(), is("{\"obj\":{\"a\":10},\"b\":11}"));
    }
}
