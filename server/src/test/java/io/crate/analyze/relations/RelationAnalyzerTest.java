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

package io.crate.analyze.relations;

import static io.crate.testing.Asserts.assertThat;
import static io.crate.testing.Asserts.isField;
import static io.crate.testing.Asserts.isFunction;
import static io.crate.testing.Asserts.isLiteral;
import static io.crate.testing.Asserts.isReference;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

import org.junit.Before;
import org.junit.Test;

import io.crate.analyze.ParamTypeHints;
import io.crate.analyze.QueriedSelectRelation;
import io.crate.exceptions.RelationUnknown;
import io.crate.exceptions.RelationValidationException;
import io.crate.expression.scalar.SubscriptFunction;
import io.crate.expression.symbol.SelectSymbol;
import io.crate.expression.symbol.Symbol;
import io.crate.expression.tablefunctions.ValuesFunction;
import io.crate.metadata.RelationName;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.SQLExecutor;
import io.crate.testing.T3;
import io.crate.types.ArrayType;
import io.crate.types.DataTypes;

public class RelationAnalyzerTest extends CrateDummyClusterServiceUnitTest {

    private SQLExecutor executor;

    @Before
    public void prepare() throws IOException {
        executor = SQLExecutor.of(clusterService)
            .addTable(T3.T1_DEFINITION)
            .addTable(T3.T2_DEFINITION)
            .addTable(T3.T3_DEFINITION);
    }

    @Test
    public void testValidateUsedRelationsInJoinConditions() {
        assertThatThrownBy(
            () -> executor.analyze("select * from t1 join t2 on t1.a = t3.c join t3 on t2.b = t3.c"))
            .isExactlyInstanceOf(RelationValidationException.class)
            .hasMessage("missing FROM-clause entry for relation '[doc.t3]'");
    }

    @Test
    public void test_can_use_array_subscript_in_order_by_referencing_alias() {
        QueriedSelectRelation relation = executor.analyze(
            "select percentile(x, [0.90, 0.95]) as percentiles from t1 order by percentiles[1]");
        List<Symbol> orderBySymbols = Objects.requireNonNull(relation.orderBy()).orderBySymbols();
        assertThat(orderBySymbols).satisfiesExactly(
            isFunction(
                SubscriptFunction.NAME,
                isFunction("percentile"),
                isLiteral(1)));

        relation = executor.analyze(
            "select percentile(x, [0.90, 0.95]) as percentiles from t1 order by percentiles[1] + 10");
        orderBySymbols = Objects.requireNonNull(relation.orderBy()).orderBySymbols();
        assertThat(orderBySymbols).satisfiesExactly(
            isFunction(
                "add",
                isFunction("subscript", isFunction("percentile"), isLiteral(1)),
                isLiteral(10.0)));
    }

    @Test
    public void testColumnNameFromArrayComparisonExpression() {
        AnalyzedRelation relation = executor.analyze("select 'foo' = any(partitioned_by) " +
            "from information_schema.tables");
        assertThat(relation.outputs().getFirst().toColumn().sqlFqn()).isEqualTo("('foo' = ANY(partitioned_by))");
    }

    @Test
    public void test_process_values_result_in_table_function_with_values_name() {
        AnalyzedRelation relation = executor.analyze("VALUES ([1, 2], 'a')");
        assertThat(relation).isExactlyInstanceOf(TableFunctionRelation.class);
        assertThat(relation.relationName()).hasToString(ValuesFunction.NAME);
    }

    /// See bug: [ARRAY inner types: When inserting multiple records, validation is skipped on the first record](https://github.com/crate/crate/issues/19231)
    @Test
    public void test_insert_values_with_incompatible_types() throws Exception {
        executor.addTable("create table t01 (data array(string));");
        var objArrayType = new ArrayType<>(DataTypes.UNTYPED_OBJECT);

        assertThatThrownBy(() -> executor.analyze(
            "insert into t01 (data) values (?), (?);",
            new ParamTypeHints(List.of(objArrayType, objArrayType))
        ))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("Cannot convert VALUES element in row 1 of type `object_array` to `text_array` for `data`");


        assertThatThrownBy(() -> executor.analyze(
            "insert into t01 (data) values (?);",
            new ParamTypeHints(List.of(objArrayType))
        ))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("Cannot convert VALUES element in row 1 of type `object_array` to `text_array` for `data`");

    }

    @Test
    public void test_fqn_with_catalog() {
        AnalyzedRelation relation = executor.analyze("select * from crate.doc.t1");
        assertThat(relation.outputs()).hasSize(3);

        relation = executor.analyze("select crate.doc.t1.a from crate.doc.t1");
        assertThat(relation.outputs()).hasSize(1);
        assertThat(relation.outputs().getFirst().toColumn().fqn()).isEqualTo("a");

        relation = executor.analyze("select crate.doc.t1.a from t1");
        assertThat(relation.outputs()).hasSize(1);
        assertThat(relation.outputs().getFirst().toColumn().fqn()).isEqualTo("a");

        relation = executor.analyze("select t.a from crate.doc.t1 as t");
        assertThat(relation.outputs()).hasSize(1);
        assertThat(relation.outputs().getFirst().toColumn().fqn()).isEqualTo("a");
    }

    @Test
    public void test_fqn_with_invalid_catalog() {
        assertThatThrownBy(
            () -> executor.analyze("select * from \"invalidCatalog\".doc.t1"))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("Unexpected catalog name: invalidCatalog. Only available catalog is crate");
        assertThatThrownBy(
            () -> executor.analyze("select invalid.doc.t1.a from crate.doc.t1"))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("Unexpected catalog name: invalid. Only available catalog is crate");
    }

    // tracks a bug: https://github.com/crate/crate/issues/15516
    @Test
    public void test_resolve_relations_by_going_through_each_search_path_at_a_time() throws IOException {
        var executor = SQLExecutor.of(clusterService)
            .addTable("create table b.t1 (x text);")
            .addView(new RelationName("a", "t1"), "select 'view'")
            .setSearchPath("a", "b");

        QueriedSelectRelation relation = executor.analyze("select * from t1");
        assertThat(relation.from()).hasSize(1);
        assertThat(relation.from().getFirst()).isInstanceOf(AnalyzedView.class);
    }

    @Test
    public void test_with_query_takes_precedence_over_tables_and_views_with_same_name() throws IOException {
        var executor = SQLExecutor.of(clusterService)
            .addTable("create table tbl (k int, other int)")
            .addView(new RelationName("doc", "v"), "select 1 as x");

        QueriedSelectRelation relation = executor.analyze(
            "WITH tbl(a, b) AS (VALUES (1, 100)) SELECT tbl.b FROM tbl");
        assertThat(relation.from().getFirst()).isExactlyInstanceOf(AliasedAnalyzedRelation.class);
        assertThat(relation.outputs()).satisfiesExactly(isField("b", new RelationName(null, "tbl")));

        relation = executor.analyze(
            "WITH v(c) AS (VALUES (100)) SELECT v.c FROM v");
        assertThat(relation.from().getFirst()).isExactlyInstanceOf(AliasedAnalyzedRelation.class);
        assertThat(relation.outputs()).satisfiesExactly(isField("c", new RelationName(null, "v")));
    }

    @Test
    public void test_schema_qualified_name_resolves_to_table_and_not_to_with_query() throws IOException {
        var executor = SQLExecutor.of(clusterService)
            .addTable("create table tbl (a int, b int)");

        QueriedSelectRelation relation = executor.analyze(
            "WITH tbl (a, b) AS (VALUES (1, 100)) SELECT tbl.b FROM doc.tbl");
        assertThat(relation.from().getFirst().relationName()).isEqualTo(new RelationName("doc", "tbl"));
        assertThat(relation.outputs()).satisfiesExactly(isReference("b"));
    }

    @Test
    public void test_with_query_is_visible_within_nested_subqueries() {
        QueriedSelectRelation relation = executor.analyze(
            "WITH tbl AS (SELECT 1 AS x) SELECT (SELECT max(x) FROM tbl) FROM t1");
        assertThat(relation.outputs()).satisfiesExactly(
            s -> assertThat(s).isExactlyInstanceOf(SelectSymbol.class));
    }

    @Test
    public void test_correlated_subquery_resolves_parent_column_if_statement_has_with_queries() {
        QueriedSelectRelation relation = executor.analyze(
            "WITH c AS (SELECT 1 AS x) " +
            "SELECT t1.a, (SELECT count(*) FROM t2 WHERE t2.b = t1.a) FROM t1");
        assertThat(relation.outputs()).satisfiesExactly(
            isReference("a"),
            s -> assertThat(s).isExactlyInstanceOf(SelectSymbol.class)
        );
    }

    @Test
    public void test_columns_of_with_query_are_not_accessible_without_using_it_in_from() {
        assertThatThrownBy(() -> executor.analyze("WITH tbl AS (SELECT 1 AS x) SELECT tbl.x FROM t1"))
            .isExactlyInstanceOf(RelationUnknown.class)
            .hasMessage("Relation 'doc.tbl' unknown");
    }

    @Test
    public void test_duplicate_with_query_names_are_rejected() {
        assertThatThrownBy(() -> executor.analyze("WITH tbl AS (SELECT 1), tbl AS (SELECT 2) SELECT * FROM tbl"))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("WITH query name \"tbl\" specified more than once");
    }
}
