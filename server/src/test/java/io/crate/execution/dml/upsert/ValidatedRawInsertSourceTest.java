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

import static io.crate.testing.SymbolMatchers.isDynamicReference;
import static io.crate.testing.SymbolMatchers.isReference;
import static io.crate.testing.TestingHelpers.createReference;
import static java.util.Collections.singletonList;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.elasticsearch.index.mapper.StrictDynamicMappingException;
import org.hamcrest.Matchers;
import org.junit.Before;
import org.junit.Test;

import io.crate.analyze.QueriedSelectRelation;
import io.crate.analyze.relations.DocTableRelation;
import io.crate.common.collections.Maps;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.CoordinatorTxnCtx;
import io.crate.metadata.PartitionName;
import io.crate.metadata.Reference;
import io.crate.metadata.SimpleReference;
import io.crate.metadata.TransactionContext;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.Asserts;
import io.crate.testing.SQLExecutor;
import io.crate.types.DataTypes;

public class ValidatedRawInsertSourceTest extends CrateDummyClusterServiceUnitTest {

    private SQLExecutor e;
    private DocTableInfo t1;
    private DocTableInfo t2;
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

        relation = e.analyze("select obj, b from t2");
        t2 = ((DocTableRelation) relation.from().get(0)).tableInfo();
    }

    @Test
    public void test_invalid_source_with_extra_trailing_chars() throws Exception {
        var e = SQLExecutor.builder(clusterService)
            .addTable("create table t (a int)")
            .build();
        DocTableInfo t = e.resolveTableInfo("t");
        ValidatedRawInsertSource insertSource = new ValidatedRawInsertSource(t, txnCtx, e.nodeCtx, "t");
        Asserts.assertThrowsMatches(
            () -> insertSource.generateSourceAndCheckConstraints(new Object[]{"{}}"}, List.of()), // trailing '}'
            IllegalArgumentException.class,
            "failed to parse `}`"
        );
    }

    @Test
    public void test_type_validation_string_to_boolean() throws Exception {
        var e = SQLExecutor.builder(clusterService)
            .addTable("create table t (b boolean)")
            .build();
        DocTableInfo t = e.resolveTableInfo("t");
        ValidatedRawInsertSource insertSource = new ValidatedRawInsertSource(t, txnCtx, e.nodeCtx, "t");
        Asserts.assertThrowsMatches(
            () -> insertSource.generateSourceAndCheckConstraints(new Object[]{"{\"b\": \"\"}"}, List.of()),
            IllegalArgumentException.class,
            "Cannot cast value `` to type `boolean`"
        );
    }

    @Test
    public void test_copy_from_unknown_column_to_strict_table() throws Exception {
        var e = SQLExecutor.builder(clusterService)
            .addTable("create table t (a int) with (column_policy='strict')")
            .build();
        DocTableInfo t = e.resolveTableInfo("t");
        ValidatedRawInsertSource insertSource = new ValidatedRawInsertSource(t, txnCtx, e.nodeCtx, "t");
        Asserts.assertThrowsMatches(
            () -> insertSource.generateSourceAndCheckConstraints(new Object[]{"{\"b\": \"\"}"}, List.of()),
            StrictDynamicMappingException.class,
            "mapping set to strict, dynamic introduction of [b] within [doc.t] is not allowed"
        );
    }

    @Test
    public void test_refLookUpCache_with_multiple_invocations_of_generateSourceAndCheckConstraints() throws Exception {
        var e = SQLExecutor.builder(clusterService)
            .addTable("create table t (x int, y as x + 1, z as x + 2) with (column_policy = 'dynamic')")
            .build();
        DocTableInfo t = e.resolveTableInfo("t");
        ValidatedRawInsertSource insertSource = new ValidatedRawInsertSource(t, txnCtx, e.nodeCtx, "t");

        // iteration 1
        Map<String, Object> map = insertSource.generateSourceAndCheckConstraints(new Object[]{"{\"x\":1, \"y\":2}"}, List.of());
        assertThat(Maps.getByPath(map, "x"), is(1));
        assertThat(Maps.getByPath(map, "y"), is(2));
        assertThat(Maps.getByPath(map, "z"), is(3));
        assertThat(insertSource.refLookUpCache.lookupCache.size(), is (2));
        assertThat(insertSource.refLookUpCache.lookupCache.get("x"), isReference("x", DataTypes.INTEGER)); // lookupCache populated from iteration 1
        assertThat(insertSource.refLookUpCache.lookupCache.get("y"), isReference("y", DataTypes.INTEGER)); // lookupCache populated from iteration 1
        assertThat(insertSource.refLookUpCache.presentColumns.size(), is(0)); // check that it is cleared

        // iteration 2
        map = insertSource.generateSourceAndCheckConstraints(new Object[]{"{\"x\":10}"}, List.of());
        assertThat(Maps.getByPath(map, "x"), is(10));
        assertThat(Maps.getByPath(map, "y"), is(11));
        assertThat(Maps.getByPath(map, "z"), is(12));
        assertThat(insertSource.refLookUpCache.lookupCache.size(), is (2));
        assertThat(insertSource.refLookUpCache.lookupCache.get("x"), isReference("x", DataTypes.INTEGER)); // lookupCache populated from iteration 1
        assertThat(insertSource.refLookUpCache.lookupCache.get("y"), isReference("y", DataTypes.INTEGER)); // lookupCache populated from iteration 1
        assertThat(insertSource.refLookUpCache.presentColumns.size(), is(0)); // check that it is cleared

        // iteration 3
        map = insertSource.generateSourceAndCheckConstraints(new Object[]{"{\"x\":20, \"newCol\":\"hello\"}"}, List.of());
        assertThat(Maps.getByPath(map, "x"), is(20));
        assertThat(Maps.getByPath(map, "y"), is(21));
        assertThat(Maps.getByPath(map, "z"), is(22));
        assertThat(Maps.getByPath(map, "newCol"), is("hello"));
        assertThat(insertSource.refLookUpCache.lookupCache.size(), is (3));
        assertThat(insertSource.refLookUpCache.lookupCache.get("x"), isReference("x", DataTypes.INTEGER)); // lookupCache populated from iteration 1
        assertThat(insertSource.refLookUpCache.lookupCache.get("y"), isReference("y", DataTypes.INTEGER)); // lookupCache populated from iteration 1
        assertThat(insertSource.refLookUpCache.lookupCache.get("newCol"), isDynamicReference("newCol")); // lookupCache populated from iteration 3
        assertThat(insertSource.refLookUpCache.presentColumns.size(), is(0)); // check that it is cleared

        // iteration 4
        map = insertSource.generateSourceAndCheckConstraints(new Object[]{"{\"x\":100, \"y\":101, \"z\":102}"}, List.of());
        assertThat(Maps.getByPath(map, "x"), is(100));
        assertThat(Maps.getByPath(map, "y"), is(101));
        assertThat(Maps.getByPath(map, "z"), is(102));
        assertThat(Maps.getByPath(map, "newCol"), is(nullValue()));
        assertThat(insertSource.refLookUpCache.lookupCache.size(), is (4));
        assertThat(insertSource.refLookUpCache.lookupCache.get("x"), isReference("x", DataTypes.INTEGER)); // lookupCache populated from iteration 1
        assertThat(insertSource.refLookUpCache.lookupCache.get("y"), isReference("y", DataTypes.INTEGER)); // lookupCache populated from iteration 1
        assertThat(insertSource.refLookUpCache.lookupCache.get("newCol"), isDynamicReference("newCol")); // lookupCache populated from iteration 3
        assertThat(insertSource.refLookUpCache.lookupCache.get("z"), isReference("z", DataTypes.INTEGER)); // lookupCache populated from iteration 4
        assertThat(insertSource.refLookUpCache.presentColumns.size(), is(0)); // check that it is cleared
    }

    @Test
    public void test_lookUpCache_presentColumns() throws IOException {
        ValidatedRawInsertSource.RefLookUpCache cache = new ValidatedRawInsertSource.RefLookUpCache();
        Reference t = createReference("t", DataTypes.INTEGER);

        cache.get("t");
        assertThat(cache.presentColumns.size(), is(0)); // cache.get() of unknown columns do not mutate presentColumns

        cache.put("t", t);
        assertThat(cache.presentColumns.size(), is(1));
        assertThat(cache.presentColumns.get(0), is(t)); //cache.put() also inserts into presentColumns as well

        Asserts.assertThrowsMatches(
            () -> cache.put("t", t),
            AssertionError.class,
            "the key, value pair already exists"
        );
        assertThat(cache.presentColumns.size(), is(1));
        assertThat(cache.presentColumns.get(0), is(t));

        Asserts.assertThrowsMatches( // cache already encountered 't' from previous cache.put() calls
            () -> cache.get("t"),
            AssertionError.class,
            "for both get() and put(), the references in context should be the first encounter"
        );
        assertThat(cache.presentColumns.size(), is(1));
        assertThat(cache.presentColumns.get(0), is(t));

        cache.get("unknown");
        assertThat(cache.presentColumns.size(), is(1));
        assertThat(cache.presentColumns.get(0), is(t));

        cache.clearPresentColumns();
        assertThat(cache.presentColumns.size(), is(0));
    }

    @Test
    public void test_generated_based_on_default() throws Exception {
        var e = SQLExecutor.builder(clusterService)
            .addTable("create table generated_based_on_default (x int default 1, y as x + 1)")
            .build();
        DocTableInfo t = e.resolveTableInfo("generated_based_on_default");
        ValidatedRawInsertSource insertSource = new ValidatedRawInsertSource(t, txnCtx, e.nodeCtx, "generated_based_on_default");
        Map<String, Object> map = insertSource.generateSourceAndCheckConstraints(new Object[]{"{}"}, List.of());
        assertThat(Maps.getByPath(map, "x"), is(1));
        assertThat(Maps.getByPath(map, "y"), is(2));
    }

    @Test
    public void test_value_is_not_overwritten_by_default() throws Exception {
        var e = SQLExecutor.builder(clusterService)
            .addTable("create table generated_based_on_default (x int default 1, y as x + 1)")
            .build();
        DocTableInfo t = e.resolveTableInfo("generated_based_on_default");
        ValidatedRawInsertSource insertSource = new ValidatedRawInsertSource(t, txnCtx, e.nodeCtx, "generated_based_on_default");
        Map<String, Object> map = insertSource.generateSourceAndCheckConstraints(new Object[]{"{\"x\":2}"}, List.of());
        assertThat(Maps.getByPath(map, "x"), is(2));
        assertThat(Maps.getByPath(map, "y"), is(3));
    }

    @Test
    public void test_generate_value_text_type_with_length_exceeding_whitespaces_trimmed() throws Exception {
        var e = SQLExecutor.builder(clusterService)
            .addTable("create table t (x varchar(2) as 'ab  ')")
            .build();
        DocTableInfo t = e.resolveTableInfo("t");
        var insertSource = new ValidatedRawInsertSource(t, txnCtx, e.nodeCtx, "t");
        Map<String, Object> map = insertSource.generateSourceAndCheckConstraints(new Object[]{"{}"}, List.of());
        assertThat(Maps.getByPath(map, "x"), is("ab"));
    }

    @Test
    public void test_generate_value_that_exceeds_text_type_with_length_throws_exception() throws Exception {
        var e = SQLExecutor.builder(clusterService)
            .addTable("create table t (x varchar(2) as 'abc')")
            .build();
        DocTableInfo t = e.resolveTableInfo("t");
        var insertSource = new ValidatedRawInsertSource(t, txnCtx, e.nodeCtx, "t");
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("'abc' is too long for the text type of length: 2");
        insertSource.generateSourceAndCheckConstraints(new Object[]{"{}"}, List.of());
    }

    @Test
    public void test_default_value_that_exceeds_text_type_with_length_throws_exception() throws Exception {
        var e = SQLExecutor.builder(clusterService)
            .addTable("create table t (x varchar(2) as 'abc')")
            .build();
        DocTableInfo t = e.resolveTableInfo("t");
        var insertSource = new ValidatedRawInsertSource(t, txnCtx, e.nodeCtx, "t");
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("'abc' is too long for the text type of length: 2");
        insertSource.generateSourceAndCheckConstraints(new Object[]{"{}"}, List.of());
    }

    @Test
    public void testGenerateSourceRaisesAnErrorIfGeneratedColumnValueIsSuppliedByUserAndDoesNotMatch() throws IOException {
        ValidatedRawInsertSource sourceFromCells = new ValidatedRawInsertSource(t1, txnCtx, e.nodeCtx, "t1");

        expectedException.expectMessage("Given value 8 for generated column z does not match calculation (x + y) = 3");
        sourceFromCells.generateSourceAndCheckConstraints(new Object[]{"{\"x\":1, \"y\":2, \"z\":8}"}, List.of());
    }

    @Test
    public void testGeneratedColumnGenerationThatDependsOnNestedColumnOfObject() throws IOException {
        ValidatedRawInsertSource sourceFromCells = new ValidatedRawInsertSource(t2, txnCtx, e.nodeCtx, "t2");
        var map = sourceFromCells.generateSourceAndCheckConstraints(new Object[]{"{\"obj\":{\"a\":10}}"}, List.of());
        assertThat(map.get("b"), is(11));
        assertThat(Maps.getByPath(map, "obj.a"), is(10));
        assertThat(Maps.getByPath(map, "obj.c"), is(13));
    }

    @Test
    public void testNullConstraintCheckCausesErrorIfRequiredPartitionedColumnValueIsNull() throws IOException {
        QueriedSelectRelation relation = e.analyze("select p from t3");
        DocTableInfo t3 = ((DocTableRelation) relation.from().get(0)).tableInfo();
        PartitionName partitionName = new PartitionName(t3.ident(), singletonList(null));

        ValidatedRawInsertSource sourceFromCells = new ValidatedRawInsertSource(t3, txnCtx, e.nodeCtx, partitionName.asIndexName());

        expectedException.expectMessage("\"p\" must not be null");
        sourceFromCells.generateSourceAndCheckConstraints(new Object[]{"{}"}, List.of());
    }

    @Test
    public void testNullConstraintCheckPassesIfRequiredPartitionedColumnValueIsNotNull() throws IOException {
        QueriedSelectRelation relation = e.analyze("select p from t3");
        DocTableInfo t3 = ((DocTableRelation) relation.from().get(0)).tableInfo();
        PartitionName partitionName = new PartitionName(t3.ident(), singletonList("10"));

        ValidatedRawInsertSource sourceFromCells = new ValidatedRawInsertSource(t3, txnCtx, e.nodeCtx, partitionName.asIndexName());

        // this must pass without error
        sourceFromCells.generateSourceAndCheckConstraints(new Object[]{"{}"}, List.of());
    }

    @Test
    public void testDefaultExpressionIsInjected() throws IOException {
        QueriedSelectRelation relation = e.analyze("select x from t4");
        DocTableInfo t4 = ((DocTableRelation) relation.from().get(0)).tableInfo();

        ValidatedRawInsertSource sourceFromCells = new ValidatedRawInsertSource(t4, txnCtx, e.nodeCtx, "t4");

        var source = sourceFromCells.generateSourceAndCheckConstraints(new Object[] {"{\"x\":1}"}, List.of());
        assertThat(source, is(Map.of("x", 1, "y", "crate")));
    }

    @Test
    public void testDefaultExpressionGivenValueOverridesDefaultValue() throws IOException {
        QueriedSelectRelation relation = e.analyze("select x, y from t4");
        DocTableInfo t4 = ((DocTableRelation) relation.from().get(0)).tableInfo();

        ValidatedRawInsertSource sourceFromCells = new ValidatedRawInsertSource(t4, txnCtx, e.nodeCtx, "t4");

        var source = sourceFromCells.generateSourceAndCheckConstraints(new Object[] {"{\"x\":1, \"y\": \"cr8\"}"}, List.of());
        assertThat(source, is(Map.of("x", 1, "y", "cr8")));
    }

    @Test
    public void test_nested_generated_column_is_provided_and_matches_computed_value() throws IOException {
        // obj object as (a int,
        //                c as obj['a'] + 3),
        // b as obj['a'] + 1
        ValidatedRawInsertSource sourceFromCells = new ValidatedRawInsertSource(t2, txnCtx, e.nodeCtx, "t2");

        var source = sourceFromCells.generateSourceAndCheckConstraints(new Object[] {"{\"obj\":{\"a\":10, \"c\":13}}"}, List.of());

        assertThat(Maps.getByPath(source, "obj.a"), is(10));
        assertThat(Maps.getByPath(source, "b"), is(11));
        assertThat(Maps.getByPath(source, "obj.c"), is(13));
    }

    @Test
    public void test_nested_generated_column_is_provided_and_does_not_match_computed_value() throws IOException {
        // obj object as (a int,
        //                c as obj['a'] + 3),
        // b as obj['a'] + 1
        ValidatedRawInsertSource sourceFromCells = new ValidatedRawInsertSource(t2, txnCtx, e.nodeCtx, "t2");

        expectedException.expectMessage("Given value 14 for generated column obj['c'] does not match calculation (obj['a'] + 3) = 13");
        sourceFromCells.generateSourceAndCheckConstraints(new Object[]{"{\"obj\":{\"a\":10, \"c\":14}}"}, List.of());
    }

    @Test
    public void test_nested_default_is_injected() throws Exception {
        // create table t5 (obj object as (x int default 0, y int))
        DocTableInfo t5 = e.resolveTableInfo("t5");
        Reference obj = t5.getReference(new ColumnIdent("obj"));
        assertThat(obj, Matchers.notNullValue());
        ValidatedRawInsertSource sourceFromCells = new ValidatedRawInsertSource(t5, txnCtx, e.nodeCtx, "t4");

        var source = sourceFromCells.generateSourceAndCheckConstraints(new Object[]{"{\"obj\":{\"y\":2}}"}, List.of());
        assertThat(Maps.getByPath(source, "obj.x"), is(0));
        assertThat(Maps.getByPath(source, "obj.y"), is(2));
    }

    @Test
    public void test_nested_default_expr_does_not_override_provided_values() throws Exception {
        // create table t5 (obj object as (x int default 0, y int))
        DocTableInfo t5 = e.resolveTableInfo("t5");
        Reference obj = t5.getReference(new ColumnIdent("obj"));
        assertThat(obj, Matchers.notNullValue());
        ValidatedRawInsertSource sourceFromCells = new ValidatedRawInsertSource(t5, txnCtx, e.nodeCtx, "t5");

        var source = sourceFromCells.generateSourceAndCheckConstraints(new Object[]{"{\"obj\":{\"x\":2}}"}, List.of());
        assertThat(Maps.getByPath(source, "obj.x"), is(2));
    }

    @Test
    public void test_not_null_constraint_on_generated_column() throws Exception {
        var e = SQLExecutor.builder(clusterService)
            .addTable("create table t (x int, y as x not null)")
            .build();
        DocTableInfo tableInfo = e.resolveTableInfo("t");
        ValidatedRawInsertSource sourceFromCells = new ValidatedRawInsertSource(
            tableInfo,
            txnCtx,
            e.nodeCtx,
            "t");

        var source = sourceFromCells.generateSourceAndCheckConstraints(new Object[]{"{\"x\":1}"}, List.of());

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
        ValidatedRawInsertSource sourceFromCells = new ValidatedRawInsertSource(
            tableInfo,
            txnCtx,
            e.nodeCtx,
            partition.asIndexName());

        var source = sourceFromCells.generateSourceAndCheckConstraints(new Object[]{"{}"}, List.of());

        assertThat(source, is(Map.of("x", "test")));
    }

    @Test
    public void test_default_clauses_are_evaluated_per_row() throws Exception {
        var e = SQLExecutor.builder(clusterService)
            .addTable("create table tbl (id text default gen_random_text_uuid(), x int)")
            .build();
        DocTableInfo tbl = e.resolveTableInfo("tbl");
        InsertSourceGen sourceGen = new ValidatedRawInsertSource(
            tbl,
            txnCtx,
            e.nodeCtx,
            tbl.concreteIndices()[0]
        );
        Map<String, Object> result;
        result = sourceGen.generateSourceAndCheckConstraints(new Object[] {"{\"x\":10}"}, List.of());
        var id1 = result.get("id");
        result = sourceGen.generateSourceAndCheckConstraints(new Object[] {"{\"x\":20}"}, List.of());
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
        DocTableInfo tbl = e.resolveTableInfo("tbl");
        InsertSourceGen sourceGen = new ValidatedRawInsertSource(
            tbl,
            txnCtx,
            e.nodeCtx,
            new PartitionName(tbl.ident(), List.of("1630454400000")).asIndexName()
        );
        Map<String, Object> generateSourceAndCheckConstraints =
            sourceGen.generateSourceAndCheckConstraints(new Object[] {"{\"ts\": 1631628823105, \"g_ts_month\": 1630454400000}"}, List.of());
        assertThat(generateSourceAndCheckConstraints, Matchers.hasEntry("ts", 1631628823105L));
        assertThat(generateSourceAndCheckConstraints, Matchers.hasEntry("g_ts_month", 1630454400000L));
        assertThat(generateSourceAndCheckConstraints.size(), is(2));
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
        DocTableInfo tbl = e.resolveTableInfo("tbl");
        InsertSourceGen sourceGen = new ValidatedRawInsertSource(
            tbl,
            txnCtx,
            e.nodeCtx,
            new PartitionName(tbl.ident(), List.of("1630454400000")).asIndexName()
        );
        assertThrows(IllegalArgumentException.class, () -> {
            sourceGen.generateSourceAndCheckConstraints(new Object[] {"{\"ts\": 1631628823105, \"g_ts_month\": 2630454400000}"}, List.of());
        });
    }
}
