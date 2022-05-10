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
import io.crate.common.collections.Maps;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.CoordinatorTxnCtx;
import io.crate.metadata.PartitionName;
import io.crate.metadata.Reference;
import io.crate.metadata.RelationName;
import io.crate.metadata.TransactionContext;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.SQLExecutor;
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
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.Matchers.is;

public class SourceFromCellsTest extends CrateDummyClusterServiceUnitTest {

    private SQLExecutor e;
    private DocTableInfo t1;
    private Reference x;
    private Reference y;
    private Reference z;
    private DocTableInfo t2;
    private Reference obj;
    private TransactionContext txnCtx = CoordinatorTxnCtx.systemTransactionContext();

    @Before
    public void setUpExecutor() throws Exception {
        e = SQLExecutor.builder(clusterService)
            .addTable("create table t1 (x int, y int, z as x + y)")
            .addTable("create table t2 (obj object as (a int, c as obj['a'] + 3), b as obj['a'] + 1)")
            .addPartitionedTable("create table t3 (p int not null) partitioned by (p)")
            .addTable("create table t4 (x int, y text default 'crate')")
            .addTable("create table t5 (obj object as (x int default 0, y int))")
            .addTable("create table t6 (x int default 1, y as x + 1)")
            .build();
        QueriedSelectRelation relation = e.analyze("select x, y, z from t1");
        t1 = ((DocTableRelation) relation.from().get(0)).tableInfo();
        x = (Reference) relation.outputs().get(0);
        y = (Reference) relation.outputs().get(1);
        z = (Reference) relation.outputs().get(2);

        relation = e.analyze("select obj, b from t2");
        t2 = ((DocTableRelation) relation.from().get(0)).tableInfo();
        obj = (Reference) relation.outputs().get(0);
    }

    @Test
    public void testGeneratedSourceBytesRef() throws IOException {
        InsertSourceFromCells sourceFromCells = new InsertSourceFromCells(
            txnCtx, e.nodeCtx, t1, "t1", true, Arrays.asList(x, y));
        var source = sourceFromCells.generateSourceAndCheckConstraints(new Object[]{1, 2}, List.of());
        assertThat(source, is(Map.of("x", 1, "y", 2, "z", 3)));
    }

    @Test
    public void testGenerateSourceRaisesAnErrorIfGeneratedColumnValueIsSuppliedByUserAndDoesNotMatch() throws IOException {
        InsertSourceFromCells sourceFromCells = new InsertSourceFromCells(
            txnCtx, e.nodeCtx, t1, "t1", true, Arrays.asList(x, y, z));

        expectedException.expectMessage("Given value 8 for generated column z does not match calculation (x + y) = 3");
        sourceFromCells.generateSourceAndCheckConstraints(new Object[]{1, 2, 8}, List.of());
    }

    @Test
    public void testGeneratedColumnGenerationThatDependsOnNestedColumnOfObject() throws IOException {
        InsertSourceFromCells sourceFromCells = new InsertSourceFromCells(
            txnCtx, e.nodeCtx, t2, "t2", true, Collections.singletonList(obj));
        HashMap<Object, Object> m = new HashMap<>();
        m.put("a", 10);
        var map = sourceFromCells.generateSourceAndCheckConstraints(new Object[]{m}, List.of());
        assertThat(map.get("b"), is(11));
        assertThat(Maps.getByPath(map, "obj.a"), is(10));
        assertThat(Maps.getByPath(map, "obj.c"), is(13));
    }

    @Test
    public void testNullConstraintCheckCausesErrorIfRequiredPartitionedColumnValueIsNull() throws IOException {
        QueriedSelectRelation relation = e.analyze("select p from t3");
        DocTableInfo t3 = ((DocTableRelation) relation.from().get(0)).tableInfo();
        PartitionName partitionName = new PartitionName(t3.ident(), singletonList(null));

        InsertSourceFromCells sourceFromCells = new InsertSourceFromCells(
            txnCtx, e.nodeCtx, t3, partitionName.asIndexName(), true, emptyList());

        expectedException.expectMessage("\"p\" must not be null");
        sourceFromCells.generateSourceAndCheckConstraints(new Object[0], List.of());
    }

    @Test
    public void testNullConstraintCheckPassesIfRequiredPartitionedColumnValueIsNotNull() throws IOException {
        QueriedSelectRelation relation = e.analyze("select p from t3");
        DocTableInfo t3 = ((DocTableRelation) relation.from().get(0)).tableInfo();
        PartitionName partitionName = new PartitionName(t3.ident(), singletonList("10"));

        InsertSourceFromCells sourceFromCells = new InsertSourceFromCells(
            txnCtx, e.nodeCtx, t3, partitionName.asIndexName(), true, emptyList());

        // this must pass without error
        sourceFromCells.generateSourceAndCheckConstraints(new Object[0], List.of());
    }

    @Test
    public void testDefaultExpressionIsInjected() throws IOException {
        QueriedSelectRelation relation = e.analyze("select x from t4");
        DocTableInfo t4 = ((DocTableRelation) relation.from().get(0)).tableInfo();
        Reference x = (Reference) relation.outputs().get(0);

        InsertSourceFromCells sourceFromCells = new InsertSourceFromCells(
            txnCtx, e.nodeCtx, t4, "t4", true, Arrays.asList(x));

        Object[] input = new Object[]{1};
        var source = sourceFromCells.generateSourceAndCheckConstraints(input, List.of());
        assertThat(source, is(Map.of("x", 1, "y", "crate")));
    }

    @Test
    public void testDefaultExpressionGivenValueOverridesDefaultValue() throws IOException {
        QueriedSelectRelation relation = e.analyze("select x, y from t4");
        DocTableInfo t4 = ((DocTableRelation) relation.from().get(0)).tableInfo();
        Reference x = (Reference) relation.outputs().get(0);
        Reference y = (Reference) relation.outputs().get(1);

        InsertSourceFromCells sourceFromCells = new InsertSourceFromCells(
            txnCtx, e.nodeCtx, t4, "t4", true, Arrays.asList(x, y));

        Object[] input = {1, "cr8"};
        var source = sourceFromCells.generateSourceAndCheckConstraints(input, List.of());
        assertThat(source, is(Map.of("x", 1, "y", "cr8")));
    }

    @Test
    public void test_nested_generated_column_is_provided_and_matches_computed_value() throws IOException {
        // obj object as (a int,
        //                c as obj['a'] + 3),
        // b as obj['a'] + 1
        List<Reference> targets = List.of(obj);
        InsertSourceFromCells sourceFromCells = new InsertSourceFromCells(
            txnCtx, e.nodeCtx, t2, "t2", true, targets);
        HashMap<String, Object> providedValueForObj = new HashMap<>();
        providedValueForObj.put("a", 10);
        providedValueForObj.put("c", 13);

        var source = sourceFromCells.generateSourceAndCheckConstraints(new Object[]{providedValueForObj}, List.of());

        assertThat(Maps.getByPath(source, "obj.a"), is(10));
        assertThat(Maps.getByPath(source, "b"), is(11));
        assertThat(Maps.getByPath(source, "obj.c"), is(13));
    }

    @Test
    public void test_nested_generated_column_is_provided_and_does_not_match_computed_value() throws IOException {
        // obj object as (a int,
        //                c as obj['a'] + 3),
        // b as obj['a'] + 1
        List<Reference> targets = List.of(obj);
        InsertSourceFromCells sourceFromCells = new InsertSourceFromCells(
            txnCtx, e.nodeCtx, t2, "t2", true, targets);
        HashMap<String, Object> providedValueForObj = new HashMap<>();
        providedValueForObj.put("a", 10);
        providedValueForObj.put("c", 14);

        expectedException.expectMessage("Given value 14 for generated column obj['c'] does not match calculation (obj['a'] + 3) = 13");
        sourceFromCells.generateSourceAndCheckConstraints(new Object[]{providedValueForObj}, List.of());
    }

    @Test
    public void test_nested_default_is_injected() throws Exception {
        // create table t5 (obj object as (x int default 0, y int))
        DocTableInfo t5 = e.resolveTableInfo("t5");
        Reference obj = t5.getReference(new ColumnIdent("obj"));
        assertThat(obj, Matchers.notNullValue());
        List<Reference> targets = List.of(obj);
        InsertSourceFromCells sourceFromCells = new InsertSourceFromCells(
            txnCtx, e.nodeCtx, t5, "t4", true, targets);
        HashMap<String, Object> providedValueForObj = new HashMap<>();
        providedValueForObj.put("y", 2);
        var source = sourceFromCells.generateSourceAndCheckConstraints(new Object[]{providedValueForObj}, List.of());
        assertThat(Maps.getByPath(source, "obj.x"), is(0));
        assertThat(Maps.getByPath(source, "obj.y"), is(2));
    }

    @Test
    public void test_nested_default_expr_does_not_override_provided_values() throws Exception {
        // create table t5 (obj object as (x int default 0, y int))
        DocTableInfo t5 = e.resolveTableInfo("t5");
        Reference obj = t5.getReference(new ColumnIdent("obj"));
        assertThat(obj, Matchers.notNullValue());
        List<Reference> targets = List.of(obj);
        InsertSourceFromCells sourceFromCells = new InsertSourceFromCells(
            txnCtx, e.nodeCtx, t5, "t5", true, targets);
        HashMap<String, Object> providedValueForObj = new HashMap<>();
        providedValueForObj.put("x", 2);
        var source = sourceFromCells.generateSourceAndCheckConstraints(new Object[]{providedValueForObj}, List.of());
        assertThat(Maps.getByPath(source, "obj.x"), is(2));
    }

    @Test
    public void test_generated_based_on_default() throws Exception {
        DocTableInfo t6 = e.resolveTableInfo("t6");
        InsertSourceFromCells sourceFromCells = new InsertSourceFromCells(
            txnCtx, e.nodeCtx, t6, "t6", true, List.of());
        var source = sourceFromCells.generateSourceAndCheckConstraints(new Object[0], List.of());
        assertThat(Maps.getByPath(source, "x"), is(1));
        assertThat(Maps.getByPath(source, "y"), is(2));
    }

    @Test
    public void test_not_null_constraint_on_generated_column() throws Exception {
        var e = SQLExecutor.builder(clusterService)
            .addTable("create table t (x int, y as x not null)")
            .build();
        DocTableInfo tableInfo = e.resolveTableInfo("t");
        InsertSourceFromCells sourceFromCells = new InsertSourceFromCells(
            txnCtx,
            e.nodeCtx,
            tableInfo,
            "t",
            true,
            List.of(Objects.requireNonNull(tableInfo.getReference(new ColumnIdent("x")))));

        var source = sourceFromCells.generateSourceAndCheckConstraints(new Object[]{1}, List.of());

        assertThat(source, is(Map.of("x", 1, "y", 1)));
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
            e.nodeCtx,
            tableInfo,
            partition.asIndexName(),
            true,
            List.of());

        var source = sourceFromCells.generateSourceAndCheckConstraints(new Object[]{}, List.of());

        assertThat(source, is(Map.of("x", "test")));
    }

    @Test
    public void test_can_insert_not_null_value_on_child_of_object_array_with_not_null_constraint() throws IOException {
        var e = SQLExecutor.builder(clusterService)
            .addTable("create table tbl (payloads array(object(strict) as (x integer not null)) not null)")
            .build();
        DocTableInfo tableInfo = e.resolveTableInfo("tbl");
        InsertSourceGen sourceGen = InsertSourceGen.of(
            txnCtx,
            e.nodeCtx,
            tableInfo,
            tableInfo.concreteIndices()[0],
            true,
            List.copyOf(tableInfo.columns())
        );
        var payloads = List.of(Map.of("x", 10), Map.of("x", 20));
        var source = sourceGen.generateSourceAndCheckConstraints(new Object[] { payloads }, List.of());
        assertThat(source, is(Map.of("payloads", List.of(Map.of("x", 10), Map.of("x", 20)))));
    }

    @Test
    public void test_nested_partition_column_is_not_included_in_the_source() throws Exception {
        var e = SQLExecutor.builder(clusterService)
            .addPartitionedTable(
                "create table doc.tbl (obj object as (p integer, x integer)) partitioned by (obj['p'])",
                new PartitionName(new RelationName("doc", "tbl"), List.of("1")).asIndexName()
            )
            .build();
        DocTableInfo table = e.resolveTableInfo("tbl");
        InsertSourceGen sourceGen = InsertSourceGen.of(
            txnCtx,
            e.nodeCtx,
            table,
            table.concreteIndices()[0],
            true,
            List.copyOf(table.columns())
        );
        Map<String, Object> obj = new HashMap<>();
        obj.put("x", 10);
        obj.put("p", 1);
        var source = sourceGen.generateSourceAndCheckConstraints(new Object[] { obj }, List.of());
        assertThat(source, is(Map.of("obj", Map.of("x", 10))));
    }

    @Test
    public void test_generate_value_text_type_with_length_exceeding_whitespaces_trimmed() throws IOException {
        var e = SQLExecutor.builder(clusterService)
            .addTable("create table tbl (str varchar(2) as 'ab ')")
            .build();
        DocTableInfo t = e.resolveTableInfo("tbl");
        InsertSourceGen sourceGen = InsertSourceGen.of(
            txnCtx,
            e.nodeCtx,
            t,
            t.concreteIndices()[0],
            true,
            List.of()
        );
        var source = sourceGen.generateSourceAndCheckConstraints(new Object[]{}, List.of());
        assertThat(source, is(Map.of("str", "ab")));
    }

    @Test
    public void test_generate_value_that_exceeds_text_type_with_length_throws_exception() throws IOException {
        var e = SQLExecutor.builder(clusterService)
            .addTable("create table tbl (str varchar(1) default 'ab')")
            .build();
        DocTableInfo t = e.resolveTableInfo("tbl");
        InsertSourceGen sourceGen = InsertSourceGen.of(
            txnCtx,
            e.nodeCtx,
            t,
            t.concreteIndices()[0],
            true,
            List.of()
        );
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("'ab' is too long for the text type of length: 1");
        sourceGen.generateSourceAndCheckConstraints(new Object[]{}, List.of());
    }

    @Test
    public void test_default_value_that_exceeds_text_type_with_length_throws_exception() throws IOException {
        var e = SQLExecutor.builder(clusterService)
            .addTable("create table tbl (str varchar(1) as 'ab ')")
            .build();
        DocTableInfo t = e.resolveTableInfo("tbl");
        InsertSourceGen sourceGen = InsertSourceGen.of(
            txnCtx,
            e.nodeCtx,
            t,
            t.concreteIndices()[0],
            true,
            List.of()
        );
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("'ab ' is too long for the text type of length: 1");
        sourceGen.generateSourceAndCheckConstraints(new Object[]{}, List.of());
    }

    @Test
    public void test_keys_with_null_values_are_excluded_from_source() throws Exception {
        var e = SQLExecutor.builder(clusterService)
            .addTable("create table robots (name text not null, model text)")
            .build();
        Reference name = (Reference) e.asSymbol("name");
        Reference model = (Reference) e.asSymbol("model");
        DocTableInfo robotsTable = e.resolveTableInfo("robots");
        InsertSourceGen sourceGen = InsertSourceGen.of(
            txnCtx,
            e.nodeCtx,
            robotsTable,
            robotsTable.concreteIndices()[0],
            true,
            List.of(name, model)
        );
        Map<String, Object> source = sourceGen.generateSourceAndCheckConstraints(new Object[] { "foo", null }, List.of());
        assertThat(source, not(Matchers.hasKey("model")));
    }

    @Test
    public void test_default_clauses_are_evaluated_per_row() throws Exception {
        var e = SQLExecutor.builder(clusterService)
            .addTable("create table tbl (id text default gen_random_text_uuid(), x int)")
            .build();
        Reference x = (Reference) e.asSymbol("tbl.x");
        DocTableInfo tbl = e.resolveTableInfo("tbl");
        InsertSourceGen sourceGen = InsertSourceGen.of(
            txnCtx,
            e.nodeCtx,
            tbl,
            tbl.concreteIndices()[0],
            false,
            List.of(x)
        );
        Map<String, Object> result;
        result = sourceGen.generateSourceAndCheckConstraints(new Object[] { 10 }, List.of());
        var id1 = result.get("id");
        result = sourceGen.generateSourceAndCheckConstraints(new Object[] { 20 }, List.of());
        var id2 = result.get("id");
        assertThat(id1, Matchers.notNullValue());
        assertThat(id1, Matchers.not(is(id2)));
    }

    @Test
    public void test_generated_partition_column_passes_validation_if_value_matches() throws Exception {
        var e = SQLExecutor.builder(clusterService)
            .addPartitionedTable("""
                create table tbl (
                    ts timestamp with time zone,
                    g_ts_month as date_trunc('month', ts)
                ) partitioned by (g_ts_month)
            """)
            .build();
        Reference ts = (Reference) e.asSymbol("tbl.ts");
        Reference g_ts_month = (Reference) e.asSymbol("tbl.g_ts_month");
        DocTableInfo tbl = e.resolveTableInfo("tbl");
        InsertSourceGen sourceGen = InsertSourceGen.of(
            txnCtx,
            e.nodeCtx,
            tbl,
            new PartitionName(tbl.ident(), List.of("1630454400000")).asIndexName(),
            true,
            List.of(ts, g_ts_month)
        );
        Map<String, Object> generateSourceAndCheckConstraints =
            sourceGen.generateSourceAndCheckConstraints(new Object[] { 1631628823105L, 1630454400000L }, List.of());
        assertThat(generateSourceAndCheckConstraints, Matchers.hasEntry("ts", 1631628823105L));
        assertThat("partition value must not become part of the source", generateSourceAndCheckConstraints.size(), is(1));
    }

    @Test
    public void test_generated_partition_column_fails_validation_if_value_is_wrong() throws Exception {
        var e = SQLExecutor.builder(clusterService)
            .addPartitionedTable("""
                create table tbl (
                    ts timestamp with time zone,
                    g_ts_month as date_trunc('month', ts)
                ) partitioned by (g_ts_month)
            """)
            .build();
        Reference ts = (Reference) e.asSymbol("tbl.ts");
        Reference g_ts_month = (Reference) e.asSymbol("tbl.g_ts_month");
        DocTableInfo tbl = e.resolveTableInfo("tbl");
        InsertSourceGen sourceGen = InsertSourceGen.of(
            txnCtx,
            e.nodeCtx,
            tbl,
            new PartitionName(tbl.ident(), List.of("1630454400000")).asIndexName(),
            true,
            List.of(ts, g_ts_month)
        );
        assertThrows(IllegalArgumentException.class, () -> {
            sourceGen.generateSourceAndCheckConstraints(new Object[] { 1631628823105L, 2630454400000L }, List.of());
        });
    }
}
