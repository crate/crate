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

import static io.crate.testing.SymbolMatchers.isField;
import static io.crate.testing.SymbolMatchers.isLiteral;
import static io.crate.testing.SymbolMatchers.isReference;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import java.io.IOException;

import org.hamcrest.Matchers;
import org.junit.Before;
import org.junit.Test;

import io.crate.analyze.relations.UnionSelect;
import io.crate.exceptions.ColumnUnknownException;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.SQLExecutor;
import io.crate.testing.SymbolMatchers;
import io.crate.types.DataTypes;
import io.crate.types.ObjectType;

public class UnionAnalyzerTest extends CrateDummyClusterServiceUnitTest {

    private SQLExecutor sqlExecutor;

    @Before
    public void prepare() throws IOException {
        sqlExecutor = SQLExecutor.builder(clusterService)
            .addTable(TableDefinitions.USER_TABLE_DEFINITION)
            .addTable(TableDefinitions.USER_TABLE_MULTI_PK_DEFINITION)
            .build();
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
        assertThat(relation.orderBy().orderBySymbols(), contains(isField("id"), isField("text")));
        assertThat(relation.limit(), isLiteral(10L));
        assertThat(relation.offset(), isLiteral(20L));
        assertThat(relation.isDistinct(), is(false));

        UnionSelect tableUnion = ((UnionSelect) relation.from().get(0));
        assertThat(tableUnion.left(), instanceOf(QueriedSelectRelation.class));
        assertThat(tableUnion.right(), instanceOf(QueriedSelectRelation.class));
        assertThat(tableUnion.outputs(), contains(isField("id"), isField("text")));
        assertThat(tableUnion.left().outputs(), contains(isReference("id"), isReference("text")));
        assertThat(tableUnion.right().outputs(), contains(isReference("id"), isReference("name")));
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
        assertThat(relation.orderBy().orderBySymbols(), contains(isField("text")));
        assertThat(relation.limit(), isLiteral(10L));
        assertThat(relation.offset(), isLiteral(20L));
        assertThat(relation.isDistinct(), is(false));

        UnionSelect tableUnion1 = ((UnionSelect) relation.from().get(0));
        assertThat(tableUnion1.left(), instanceOf(UnionSelect.class));
        assertThat(tableUnion1.right(), instanceOf(QueriedSelectRelation.class));
        assertThat(tableUnion1.outputs(), contains(isField("id"), isField("text")));
        assertThat(tableUnion1.right().outputs(), contains(isReference("id"), isReference("name")));

        UnionSelect tableUnion2 = (UnionSelect) tableUnion1.left();
        assertThat(tableUnion2.outputs(), contains(isField("id"), isField("text")));

        assertThat(tableUnion2.left(), instanceOf(QueriedSelectRelation.class));
        assertThat(tableUnion2.left().outputs(), contains(isField("id"), isField("text")));

        assertThat(tableUnion2.right(), instanceOf(QueriedSelectRelation.class));
        assertThat(tableUnion2.right().outputs(), contains(isReference("id"), isReference("name")));
    }

    @Test
    public void testUnionDistinct() throws Exception {
        SQLExecutor.builder(clusterService)
            .addTable(
                "create table x (a text)"
            ).build();

        UnionSelect unionSelect = analyze("select a from x union select a from x");
        assertThat(unionSelect.isDistinct(), is(true));

        unionSelect = analyze("select a from x union distinct select a from x");
        assertThat(unionSelect.isDistinct(), is(true));
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

        assertThat(union.outputs(), Matchers.contains(
            isField("id", DataTypes.LONG)
        ));
        assertThat(union.right().outputs(), Matchers.contains(
            isLiteral(null, DataTypes.UNDEFINED)
        ));

        union = analyze("SELECT null AS id UNION ALL SELECT id FROM users");
        assertThat(union.outputs(), Matchers.contains(
            isField("id", DataTypes.LONG)
        ));

        union = analyze("SELECT null UNION ALL SELECT id FROM users");
        assertThat(union.outputs(), Matchers.contains(
            isField("NULL", DataTypes.LONG)
        ));
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
        SQLExecutor.builder(clusterService)
            .addTable(
                "create table v1 (obj object as (col int))"
            )
            .addTable(
                "create table v2 (obj object as (col text))"
            ).build();

        UnionSelect union = analyze("select obj from v1 union all select obj from v2");
        ObjectType expectedType = ObjectType.builder().setInnerType("col", DataTypes.STRING).build();
        assertThat(
            "Output type is object(col::text) because text has higher precedence than int",
            union.outputs(),
            contains(SymbolMatchers.isField("obj", expectedType))
        );
    }

    @Test
    public void testUnionObjectTypesWithSubColumnsOfDifferentNames() throws Exception {
        SQLExecutor.builder(clusterService)
            .addTable(
                "create table v1 (obj object as (obj1 object as (col text)))"
            )
            .addTable(
                "create table v2 (obj object as (obj2 object as (col text)))"
            ).build();

        UnionSelect unionSelect = analyze("select obj from v1 union all select obj from v2");
        assertThat(unionSelect.outputs().get(0), isField("obj"));
    }
}
