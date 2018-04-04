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

package io.crate.analyze;

import io.crate.analyze.relations.OrderedLimitedRelation;
import io.crate.analyze.relations.QueriedRelation;
import io.crate.analyze.relations.UnionSelect;
import io.crate.exceptions.ColumnUnknownException;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.SQLExecutor;
import org.junit.Before;
import org.junit.Test;

import static io.crate.testing.SymbolMatchers.isLiteral;
import static io.crate.testing.TestingHelpers.isSQL;
import static org.hamcrest.Matchers.instanceOf;

public class UnionAnalyzerTest extends CrateDummyClusterServiceUnitTest {

    private SQLExecutor sqlExecutor;

    @Before
    public void prepare() {
        sqlExecutor = SQLExecutor.builder(clusterService)
            .enableDefaultTables()
            .build();
    }

    private QueriedRelation analyze(String statement) {
        return sqlExecutor.analyze(statement);
    }

    @Test
    public void testUnion2Tables() {
        QueriedRelation relation = analyze("select id, text from users " +
                                           "union all " +
                                           "select id, name from users_multi_pk " +
                                           "order by id, 2 " +
                                           "limit 10 offset 20");
        assertThat(relation, instanceOf(OrderedLimitedRelation.class));
        OrderedLimitedRelation orderedLimitedRelation = ((OrderedLimitedRelation) relation);
        assertThat(orderedLimitedRelation.orderBy(),
            isSQL("UnionSelect.id, UnionSelect.text"));
        assertThat(orderedLimitedRelation.limit(), isLiteral(10L));
        assertThat(orderedLimitedRelation.offset(), isLiteral(20L));

        assertThat(orderedLimitedRelation.childRelation(), instanceOf(UnionSelect.class));
        UnionSelect tableUnion = (UnionSelect) orderedLimitedRelation.childRelation();
        assertThat(tableUnion.left(), instanceOf(QueriedTable.class));
        assertThat(tableUnion.right(), instanceOf(QueriedTable.class));
        assertThat(tableUnion.querySpec(),
            isSQL("SELECT UnionSelect.id, UnionSelect.text"));
        assertThat(tableUnion.left().querySpec(), isSQL("SELECT doc.users.id, doc.users.text"));
        assertThat(tableUnion.right().querySpec(), isSQL("SELECT doc.users_multi_pk.id, doc.users_multi_pk.name"));
    }

    @Test
    public void testUnion3Tables() {
        QueriedRelation relation = analyze("select id, text from users u1 " +
                                           "union all " +
                                           "select id, name from users_multi_pk " +
                                           "union all " +
                                           "select id, name from users " +
                                           "order by text " +
                                           "limit 10 offset 20");
        assertThat(relation, instanceOf(OrderedLimitedRelation.class));
        OrderedLimitedRelation orderedLimitedRelation = ((OrderedLimitedRelation) relation);
        assertThat(orderedLimitedRelation.orderBy(), isSQL("UnionSelect.text"));
        assertThat(orderedLimitedRelation.limit(), isLiteral(10L));
        assertThat(orderedLimitedRelation.offset(), isLiteral(20L));

        assertThat(orderedLimitedRelation.childRelation(), instanceOf(UnionSelect.class));
        UnionSelect tableUnion1 = (UnionSelect) orderedLimitedRelation.childRelation();
        assertThat(tableUnion1.left(), instanceOf(UnionSelect.class));
        assertThat(tableUnion1.right(), instanceOf(QueriedTable.class));
        assertThat(tableUnion1.querySpec(),
            isSQL("SELECT UnionSelect.id, UnionSelect.text"));
        assertThat(tableUnion1.right().querySpec(), isSQL("SELECT doc.users.id, doc.users.name"));

        UnionSelect tableUnion2 = (UnionSelect) tableUnion1.left();
        assertThat(tableUnion2.querySpec(),
            isSQL("SELECT UnionSelect.id, UnionSelect.text"));
        assertThat(tableUnion2.left(), instanceOf(QueriedTable.class));
        assertThat(tableUnion2.right(), instanceOf(QueriedTable.class));
        assertThat(tableUnion2.left().querySpec(), isSQL("SELECT doc.users.id, doc.users.text"));
        assertThat(tableUnion2.right().querySpec(), isSQL("SELECT doc.users_multi_pk.id, doc.users_multi_pk.name"));
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
        expectedException.expectMessage("Corresponding output columns at position: 2 " +
                                        "must be compatible for all parts of a UNION");
        analyze("select 1, 2 from users " +
                "union all " +
                "select 3, friends from users_multi_pk");
    }

    @Test
    public void testUnionWithNullOutput() {
        expectedException.expect(UnsupportedOperationException.class);
        expectedException.expectMessage("Corresponding output columns at position: 1 " +
                                        "must be compatible for all parts of a UNION");
        analyze("select id from users " +
                "union all " +
                "select null");
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
}
