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

import io.crate.analyze.relations.QueriedRelation;
import io.crate.analyze.symbol.SelectSymbol;
import io.crate.operation.operator.EqOperator;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.SQLExecutor;
import org.junit.Before;
import org.junit.Test;

import static io.crate.testing.SymbolMatchers.isFunction;
import static io.crate.testing.SymbolMatchers.isReference;
import static io.crate.testing.TestingHelpers.isSQL;
import static org.hamcrest.Matchers.instanceOf;

public class SingleRowSubselectAnalyzerTest extends CrateDummyClusterServiceUnitTest {

    private SQLExecutor e;

    @Before
    public void prepare() {
        e = SQLExecutor.builder(clusterService).enableDefaultTables().build();
    }

    @Test
    public void testSingleRowSubselectInWhereClause() throws Exception {
        QueriedRelation relation = e.analyze("select * from t1 where x = (select y from t2)");
        assertThat(relation.where().query(),
            isSQL("(doc.t1.x = SelectSymbol{integer_table})"));
    }

    @Test
    public void testSingleRowSubselectInWhereClauseNested() throws Exception {
        QueriedRelation relation = e.analyze(
            "select a from t1 where x = (select y from t2 where y = (select z from t3))");
        assertThat(relation.where().query(),
            isSQL("(doc.t1.x = SelectSymbol{integer_table})"));
    }

    @Test
    public void testSingleRowSubselectInSelectList() throws Exception {
        QueriedRelation relation = e.analyze("select (select b from t2 limit 1) from t1");
        assertThat(relation.outputs(), isSQL("SelectSymbol{string_table}"));
    }

    @Test
    public void testSubselectWithMultipleColumns() throws Exception {
        expectedException.expectMessage("Subqueries with more than 1 column are not supported.");
        e.analyze("select (select b, b from t2 limit 1) from t1");
    }

    @Test
    public void testSingleRowSubselectInAssignmentOfUpdate() throws Exception {
        expectedException.expectMessage("Subquery not supported in this statement");
        e.analyze("update t1 set x = (select y from t2)");
        /*
        assertThat(
            stmt.nestedStatements().get(0).assignments().values().iterator().next(),
            Matchers.instanceOf(SelectSymbol.class));
            */
    }

    @Test
    public void testSingleRowSubselectInWhereClauseOfDelete() throws Exception {
        AnalyzedDeleteStatement delete = e.analyze("delete from t1 where x = (select y from t2)");
        assertThat(delete.query(), isFunction(EqOperator.NAME, isReference("x"), instanceOf(SelectSymbol.class)));
    }

    @Test
    public void testMatchPredicateWithSingleRowSubselect() throws Exception {
        QueriedRelation relation = e.analyze(
            "select * from users where match(shape 1.2, (select shape from users limit 1))");
        assertThat(relation.where().query(),
            isSQL("match({\"shape\"=1.2}, SelectSymbol{geo_shape_table}, 'intersects', {})"));
    }

    @Test
    public void testLikeSupportsSubQueries() throws Exception {
        QueriedRelation relation = e.analyze("select * from users where name like (select 'foo')");
        assertThat(relation.where().query(),
            isSQL("(doc.users.name LIKE SelectSymbol{string_table})"));
    }

    @Test
    public void testAnySupportsSubQueries() throws Exception {
        QueriedRelation relation = e.analyze("select * from users where (select 'bar') = ANY (tags)");
        assertThat(relation.where().query(),
            isSQL("(SelectSymbol{string_table} = ANY(doc.users.tags))"));

        relation = e.analyze("select * from users where 'bar' = ANY (select 'bar')");
        assertThat(relation.where().query(),
            isSQL("('bar' = ANY(SelectSymbol{string_table}))"));
    }
}
