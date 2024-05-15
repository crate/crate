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

package io.crate.analyze;

import static io.crate.testing.Asserts.isField;
import static io.crate.testing.Asserts.isLiteral;
import static io.crate.testing.Asserts.isReference;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertThat;

import java.io.IOException;
import java.util.Objects;

import org.junit.Before;
import org.junit.Test;

import io.crate.analyze.relations.UnionSelect;
import io.crate.exceptions.AmbiguousColumnException;
import io.crate.exceptions.ColumnUnknownException;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.Asserts;
import io.crate.testing.SQLExecutor;
import io.crate.types.DataTypes;
import io.crate.types.ObjectType;

public class UnionAnalyzerTest extends CrateDummyClusterServiceUnitTest {

    private SQLExecutor sqlExecutor;

    @Before
    public void prepare() throws IOException {
        sqlExecutor = SQLExecutor.of(clusterService)
            .addTable(TableDefinitions.USER_TABLE_DEFINITION)
            .addTable(TableDefinitions.USER_TABLE_MULTI_PK_DEFINITION);
    }

    private <T extends AnalyzedStatement> T analyze(String statement) {
        return sqlExecutor.analyze(statement);
    }

    @Test
    public void testUnion2Tables() {
        QueriedSelectRelation relation = analyze(
            "select id, text from users " +
            "union all " +
            "select id, name from users_multi_pk " +
            "order by id, 2 " +
            "limit 10 offset 20");
        Asserts.assertThat(Objects.requireNonNull(relation.orderBy()).orderBySymbols())
            .satisfiesExactly(isField("id"), isField("text"));
        Asserts.assertThat(relation.limit()).isLiteral(10L);
        Asserts.assertThat(relation.offset()).isLiteral(20L);
        assertThat(relation.isDistinct()).isFalse();

        UnionSelect tableUnion = ((UnionSelect) relation.from().get(0));
        assertThat(tableUnion.left(), instanceOf(QueriedSelectRelation.class));
        assertThat(tableUnion.right(), instanceOf(QueriedSelectRelation.class));
        Asserts.assertThat(tableUnion.outputs()).satisfiesExactly(isField("id"), isField("text"));
        Asserts.assertThat(tableUnion.left().outputs()).satisfiesExactly(isReference("id"), isReference("text"));
        Asserts.assertThat(tableUnion.right().outputs()).satisfiesExactly(isReference("id"), isReference("name"));
    }

    @Test
    public void testUnion3Tables() {
        QueriedSelectRelation relation = analyze(
            "select id, text from users u1 " +
            "union all " +
            "select id, name from users_multi_pk " +
            "union all " +
            "select id, name from users " +
            "order by text " +
            "limit 10 offset 20"
        );
        Asserts.assertThat(Objects.requireNonNull(relation.orderBy()).orderBySymbols())
            .satisfiesExactly(isField("text"));
        Asserts.assertThat(relation.limit()).isLiteral(10L);
        Asserts.assertThat(relation.offset()).isLiteral(20L);
        assertThat(relation.isDistinct()).isFalse();

        UnionSelect tableUnion1 = ((UnionSelect) relation.from().get(0));
        assertThat(tableUnion1.left(), instanceOf(UnionSelect.class));
        assertThat(tableUnion1.right(), instanceOf(QueriedSelectRelation.class));
        Asserts.assertThat(tableUnion1.outputs()).satisfiesExactly(isField("id"), isField("text"));
        Asserts.assertThat(tableUnion1.right().outputs()).satisfiesExactly(isReference("id"), isReference("name"));

        UnionSelect tableUnion2 = (UnionSelect) tableUnion1.left();
        Asserts.assertThat(tableUnion2.outputs()).satisfiesExactly(isField("id"), isField("text"));

        assertThat(tableUnion2.left(), instanceOf(QueriedSelectRelation.class));
        Asserts.assertThat(tableUnion2.left().outputs()).satisfiesExactly(isField("id"), isField("text"));

        assertThat(tableUnion2.right(), instanceOf(QueriedSelectRelation.class));
        Asserts.assertThat(tableUnion2.right().outputs()).satisfiesExactly(isReference("id"), isReference("name"));
    }

    @Test
    public void testUnionDistinct() throws Exception {
        SQLExecutor.of(clusterService)
            .addTable("create table x (a text)");

        UnionSelect unionSelect = analyze("select a from x union select a from x");
        assertThat(unionSelect.isDistinct()).isTrue();

        unionSelect = analyze("select a from x union distinct select a from x");
        assertThat(unionSelect.isDistinct()).isTrue();
    }

    @Test
    public void testUnionDifferentNumberOfOutputs() {
        expectedException.expect(UnsupportedOperationException.class);
        expectedException.expectMessage("Number of output columns must be the same for all parts of a UNION");
        analyze("select 1, 2 from users " +
                "union all " +
                "select 3 from users_multi_pk");
    }

    @Test
    public void testUnionDifferentTypesOfOutputs() {
        expectedException.expect(UnsupportedOperationException.class);
        expectedException.expectMessage("Output columns at position 2 " +
                                        "must be compatible for all parts of a UNION. Got `integer` and `object_array`");
        analyze("select 1, 2 from users " +
                "union all " +
                "select 3, friends from users_multi_pk");
    }

    @Test
    public void testUnionWithNullOutput() {
        // NULL streaming is compatible with any type, so we can allow this
        UnionSelect union = analyze(
            "select id from users " +
            "union all " +
            "select null");

        Asserts.assertThat(union.outputs()).satisfiesExactly(isField("id", DataTypes.LONG));
        Asserts.assertThat(union.right().outputs()).satisfiesExactly(isLiteral(null, DataTypes.UNDEFINED));

        union = analyze("SELECT null AS id UNION ALL SELECT id FROM users");
        Asserts.assertThat(union.outputs()).satisfiesExactly(
            isField("id", DataTypes.LONG));

        union = analyze("SELECT null UNION ALL SELECT id FROM users");
        Asserts.assertThat(union.outputs()).satisfiesExactly(
            isField("NULL", DataTypes.LONG));
    }

    @Test
    public void testUnionOrderByRefersToColumnFromRightTable() {
        expectedException.expect(ColumnUnknownException.class);
        expectedException.expectMessage("Column name unknown");
        analyze("select id, text from users " +
                "union all " +
                "select id, name from users_multi_pk " +
                "order by name");
    }

    @Test
    public void testUnionOrderByColumnRefersToOriginalColumn() {
        expectedException.expect(ColumnUnknownException.class);
        expectedException.expectMessage("Column id unknown");
        analyze("select id + 10, text from users " +
                "union all " +
                "select id, name from users_multi_pk " +
                "order by id");
    }

    @Test
    public void testUnionObjectTypesWithSubColumnsOfSameNameButDifferentInnerTypes() throws Exception {
        SQLExecutor.of(clusterService)
            .addTable("create table v1 (obj object as (col int))")
            .addTable("create table v2 (obj object as (col text))");

        UnionSelect union = analyze("select obj from v1 union all select obj from v2");
        ObjectType expectedType = ObjectType.builder().setInnerType("col", DataTypes.INTEGER).build();
        Asserts.assertThat(union.outputs())
            .as("Output type is object(col::int) because int has higher precedence than text")
            .satisfiesExactly(isField("obj", expectedType));
    }

    @Test
    public void test_union_merging_sub_columns_for_object_types() throws Exception {
        SQLExecutor.of(clusterService)
            .addTable("create table v1 (obj object as (col1 int, col2 text))")
            .addTable("create table v2 (obj object as (col1 text, col2 int))");

        UnionSelect union = analyze("select obj from v1 union all select obj from v2");
        ObjectType expectedType = ObjectType.builder()
            .setInnerType("col1", DataTypes.INTEGER)
            .setInnerType("col2", DataTypes.INTEGER)
            .build();
        Asserts.assertThat(union.outputs())
            .as("Output type is object(col1::int, col2::int) because int has higher precedence than text")
            .satisfiesExactly(isField("obj", expectedType));
    }

    @Test
    public void testUnionObjectTypesWithSubColumnsOfDifferentNames() throws Exception {
        SQLExecutor.of(clusterService)
            .addTable("create table v1 (obj object as (obj1 object as (col text)))")
            .addTable("create table v2 (obj object as (obj2 object as (col text)))");

        UnionSelect unionSelect = analyze("select obj from v1 union all select obj from v2");
        Asserts.assertThat(unionSelect.outputs().get(0)).isField("obj");
    }

    @Test
    public void test_union_containing_duplicates_in_outputs_list_may_throw_AmbiguousColumnException() throws IOException {
        SQLExecutor.of(clusterService).addTable("create table t (a int, b int)");
        assertThatThrownBy(() -> analyze("select a from (select a, b as a from t union select 1, 1) t2"))
            .isExactlyInstanceOf(AmbiguousColumnException.class)
            .hasMessage("Column \"a\" is ambiguous");
    }
}
