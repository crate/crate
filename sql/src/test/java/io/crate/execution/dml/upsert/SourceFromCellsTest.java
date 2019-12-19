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
import io.crate.analyze.relations.AnalyzedRelation;
import io.crate.common.collections.Maps;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.CoordinatorTxnCtx;
import io.crate.metadata.PartitionName;
import io.crate.metadata.Reference;
import io.crate.metadata.TransactionContext;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.SQLExecutor;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.DeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.hamcrest.Matchers;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
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
    private TransactionContext txnCtx = CoordinatorTxnCtx.systemTransactionContext();

    @Before
    public void setUpExecutor() throws Exception {
        e = SQLExecutor.builder(clusterService)
            .addTable("create table t1 (x int, y int, z as x + y)")
            .addTable("create table t2 (obj object as (a int, c as obj['a'] + 3), b as obj['a'] + 1)")
            .addPartitionedTable("create table t3 (p int not null) partitioned by (p)")
            .addTable("create table t4 (x int, y text default 'crate')")
            .addTable("create table t5 (obj object as (x int default 0, y int))")
            .build();
        AnalyzedRelation relation = e.normalize("select x, y, z from t1");
        t1 = (DocTableInfo) ((QueriedTable) relation).tableRelation().tableInfo();
        x = (Reference) relation.outputs().get(0);
        y = (Reference) relation.outputs().get(1);
        z = (Reference) relation.outputs().get(2);

        relation = e.normalize("select obj, b from t2");
        t2 = (DocTableInfo) ((QueriedTable) relation).tableRelation().tableInfo();
        obj = (Reference) relation.outputs().get(0);
        b = (Reference) relation.outputs().get(1);
    }

    @Test
    public void testGeneratedSourceBytesRef() throws IOException {
        InsertSourceFromCells sourceFromCells = new InsertSourceFromCells(
            txnCtx, e.functions(), t1, "t1", GeneratedColumns.Validation.VALUE_MATCH, Arrays.asList(x, y));
        BytesReference source = sourceFromCells.generateSourceAndCheckConstraints(new Object[]{1, 2});
        assertThat(source.utf8ToString(), is("{\"x\":1,\"y\":2,\"z\":3}"));
    }

    @Test
    public void testGenerateSourceRaisesAnErrorIfGeneratedColumnValueIsSuppliedByUserAndDoesNotMatch() throws IOException {
        InsertSourceFromCells sourceFromCells = new InsertSourceFromCells(
            txnCtx, e.functions(), t1, "t1", GeneratedColumns.Validation.VALUE_MATCH, Arrays.asList(x, y, z));

        expectedException.expectMessage("Given value 8 for generated column z does not match calculation (x + y) = 3");
        sourceFromCells.generateSourceAndCheckConstraints(new Object[]{1, 2, 8});
    }

    @Test
    public void testGeneratedColumnGenerationThatDependsOnNestedColumnOfObject() throws IOException {
        InsertSourceFromCells sourceFromCells = new InsertSourceFromCells(
            txnCtx, e.functions(), t2, "t2", GeneratedColumns.Validation.VALUE_MATCH, Collections.singletonList(obj));
        HashMap<Object, Object> m = new HashMap<>();
        m.put("a", 10);
        BytesReference source = sourceFromCells.generateSourceAndCheckConstraints(new Object[]{m});
        Map<String, Object> map = JsonXContent.jsonXContent.createParser(
            NamedXContentRegistry.EMPTY, DeprecationHandler.THROW_UNSUPPORTED_OPERATION, BytesReference.toBytes(source)).map();
        assertThat(map.get("b"), is(11));
        assertThat(Maps.getByPath(map, "obj.a"), is(10));
        assertThat(Maps.getByPath(map, "obj.c"), is(13));
    }

    @Test
    public void testNullConstraintCheckCausesErrorIfRequiredPartitionedColumnValueIsNull() throws IOException {
        AnalyzedRelation relation = e.normalize("select p from t3");
        DocTableInfo t3 = (DocTableInfo) ((QueriedTable) relation).tableRelation().tableInfo();
        PartitionName partitionName = new PartitionName(t3.ident(), singletonList(null));

        InsertSourceFromCells sourceFromCells = new InsertSourceFromCells(
            txnCtx, e.functions(), t3, partitionName.asIndexName(), GeneratedColumns.Validation.VALUE_MATCH, emptyList());

        expectedException.expectMessage("\"p\" must not be null");
        sourceFromCells.generateSourceAndCheckConstraints(new Object[0]);
    }

    @Test
    public void testNullConstraintCheckPassesIfRequiredPartitionedColumnValueIsNotNull() throws IOException {
        AnalyzedRelation relation = e.normalize("select p from t3");
        DocTableInfo t3 = (DocTableInfo) ((QueriedTable) relation).tableRelation().tableInfo();
        PartitionName partitionName = new PartitionName(t3.ident(), singletonList("10"));

        InsertSourceFromCells sourceFromCells = new InsertSourceFromCells(
            txnCtx, e.functions(), t3, partitionName.asIndexName(), GeneratedColumns.Validation.VALUE_MATCH, emptyList());

        // this must pass without error
        sourceFromCells.generateSourceAndCheckConstraints(new Object[0]);
    }

    @Test
    public void testDefaultExpressionIsInjected() throws IOException {
        AnalyzedRelation relation = e.normalize("select x from t4");
        DocTableInfo t4 = (DocTableInfo) ((QueriedTable) relation).tableRelation().tableInfo();
        Reference x = (Reference) relation.outputs().get(0);

        InsertSourceFromCells sourceFromCells = new InsertSourceFromCells(
            txnCtx, e.functions(), t4, "t4", GeneratedColumns.Validation.VALUE_MATCH, Arrays.asList(x));

        Object[] input = new Object[]{1};
        BytesReference source = sourceFromCells.generateSourceAndCheckConstraints(input);
        assertThat(source.utf8ToString(), is("{\"x\":1,\"y\":\"crate\"}"));
    }

    @Test
    public void testDefaultExpressionGivenValueOverridesDefaultValue() throws IOException {
        AnalyzedRelation relation = e.normalize("select x, y from t4");
        DocTableInfo t4 = (DocTableInfo) ((QueriedTable) relation).tableRelation().tableInfo();
        Reference x = (Reference) relation.outputs().get(0);
        Reference y = (Reference) relation.outputs().get(1);

        InsertSourceFromCells sourceFromCells = new InsertSourceFromCells(
            txnCtx, e.functions(), t4, "t4", GeneratedColumns.Validation.VALUE_MATCH, Arrays.asList(x, y));

        Object[] input = {1, "cr8"};
        BytesReference source = sourceFromCells.generateSourceAndCheckConstraints(input);
        assertThat(source.utf8ToString(), is("{\"x\":1,\"y\":\"cr8\"}"));
    }

    @Test
    public void test_nested_generated_column_is_provided_and_matches_computed_value() throws IOException {
        // obj object as (a int,
        //                c as obj['a'] + 3),
        // b as obj['a'] + 1
        List<Reference> targets = List.of(obj);
        InsertSourceFromCells sourceFromCells = new InsertSourceFromCells(
            txnCtx, e.functions(), t2, "t2", GeneratedColumns.Validation.VALUE_MATCH, targets);
        HashMap<String, Object> providedValueForObj = new HashMap<>();
        providedValueForObj.put("a", 10);
        providedValueForObj.put("c", 13);

        BytesReference source = sourceFromCells.generateSourceAndCheckConstraints(new Object[]{providedValueForObj});

        Map<String, Object> map = JsonXContent.jsonXContent.createParser(
            NamedXContentRegistry.EMPTY, DeprecationHandler.THROW_UNSUPPORTED_OPERATION, BytesReference.toBytes(source)).map();
        assertThat(Maps.getByPath(map, "obj.a"), is(10));
        assertThat(Maps.getByPath(map, "b"), is(11));
        assertThat(Maps.getByPath(map, "obj.c"), is(13));
    }

    @Test
    public void test_nested_generated_column_is_provided_and_does_not_match_computed_value() throws IOException {
        // obj object as (a int,
        //                c as obj['a'] + 3),
        // b as obj['a'] + 1
        List<Reference> targets = List.of(obj);
        InsertSourceFromCells sourceFromCells = new InsertSourceFromCells(
            txnCtx, e.functions(), t2, "t2", GeneratedColumns.Validation.VALUE_MATCH, targets);
        HashMap<String, Object> providedValueForObj = new HashMap<>();
        providedValueForObj.put("a", 10);
        providedValueForObj.put("c", 14);

        expectedException.expectMessage("Given value 14 for generated column obj['c'] does not match calculation (obj['a'] + 3) = 13");
        sourceFromCells.generateSourceAndCheckConstraints(new Object[]{providedValueForObj});
    }

    @Test
    public void test_nested_default_is_injected() throws Exception {
        // create table t5 (obj object as (x int default 0, y int))
        DocTableInfo t5 = e.resolveTableInfo("t5");
        Reference obj = t5.getReference(new ColumnIdent("obj"));
        assertThat(obj, Matchers.notNullValue());
        List<Reference> targets = List.of(obj);
        InsertSourceFromCells sourceFromCells = new InsertSourceFromCells(
            txnCtx, e.functions(), t5, "t4", GeneratedColumns.Validation.VALUE_MATCH, targets);
        HashMap<String, Object> providedValueForObj = new HashMap<>();
        providedValueForObj.put("y", 2);
        BytesReference source = sourceFromCells.generateSourceAndCheckConstraints(new Object[]{providedValueForObj});
        Map<String, Object> map = JsonXContent.jsonXContent.createParser(
            NamedXContentRegistry.EMPTY, DeprecationHandler.THROW_UNSUPPORTED_OPERATION, BytesReference.toBytes(source)).map();
        assertThat(Maps.getByPath(map, "obj.x"), is(0));
        assertThat(Maps.getByPath(map, "obj.y"), is(2));
    }

    @Test
    public void test_nested_default_expr_does_not_override_provided_values() throws Exception {
        // create table t5 (obj object as (x int default 0, y int))
        DocTableInfo t5 = e.resolveTableInfo("t5");
        Reference obj = t5.getReference(new ColumnIdent("obj"));
        assertThat(obj, Matchers.notNullValue());
        List<Reference> targets = List.of(obj);
        InsertSourceFromCells sourceFromCells = new InsertSourceFromCells(
            txnCtx, e.functions(), t5, "t4", GeneratedColumns.Validation.VALUE_MATCH, targets);
        HashMap<String, Object> providedValueForObj = new HashMap<>();
        providedValueForObj.put("x", 2);
        BytesReference source = sourceFromCells.generateSourceAndCheckConstraints(new Object[]{providedValueForObj});

        Map<String, Object> map = JsonXContent.jsonXContent.createParser(
            NamedXContentRegistry.EMPTY, DeprecationHandler.THROW_UNSUPPORTED_OPERATION, BytesReference.toBytes(source)).map();
        assertThat(Maps.getByPath(map, "obj.x"), is(2));
    }

    @Test
    public void test_not_null_constraint_on_generated_column() throws Exception {
        var e = SQLExecutor.builder(clusterService)
            .addTable("create table t (x int, y as x not null)")
            .build();
        DocTableInfo tableInfo = e.resolveTableInfo("t");
        InsertSourceFromCells sourceFromCells = new InsertSourceFromCells(
            txnCtx,
            e.functions(),
            tableInfo,
            "t",
            GeneratedColumns.Validation.VALUE_MATCH,
            List.of(Objects.requireNonNull(tableInfo.getReference(new ColumnIdent("x")))));

        BytesReference source = sourceFromCells.generateSourceAndCheckConstraints(new Object[]{1});

        assertThat(source.utf8ToString(), is("{\"x\":1,\"y\":1}"));
    }

    @Test
    public void test_not_null_constraint_on_generated_column_that_used_in_partition_by_clause() throws Exception {
        var e = SQLExecutor.builder(clusterService)
            .addPartitionedTable(
                "create table t (x as 'test' not null) " +
                "partitioned by (x)")
            .build();
        DocTableInfo tableInfo = e.resolveTableInfo("t");
        PartitionName partition = new PartitionName(tableInfo.ident(), singletonList(null));
        InsertSourceFromCells sourceFromCells = new InsertSourceFromCells(
            txnCtx,
            e.functions(),
            tableInfo,
            partition.asIndexName(),
            GeneratedColumns.Validation.VALUE_MATCH,
            List.of());

        BytesReference source = sourceFromCells.generateSourceAndCheckConstraints(new Object[]{});

        assertThat(source.utf8ToString(), is("{\"x\":\"test\"}"));
    }

    @Test
    public void test_can_insert_not_null_value_on_child_of_object_array_with_not_null_constraint() throws IOException {
        var e = SQLExecutor.builder(clusterService)
            .addTable("create table tbl (payloads array(object(strict) as (x integer not null)) not null)")
            .build();
        DocTableInfo tableInfo = e.resolveTableInfo("tbl");
        InsertSourceGen sourceGen = InsertSourceGen.of(
            txnCtx,
            e.functions(),
            tableInfo,
            tableInfo.concreteIndices()[0],
            GeneratedColumns.Validation.VALUE_MATCH,
            List.copyOf(tableInfo.columns())
        );
        var payloads = List.of(Map.of("x", 10), Map.of("x", 20));
        var source = sourceGen.generateSourceAndCheckConstraints(new Object[] { payloads });
        assertThat(source.utf8ToString(), is("{\"payloads\":[{\"x\":10},{\"x\":20}]}"));
    }
}
