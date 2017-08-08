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

import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.SQLExecutor;
import org.hamcrest.Matchers;
import org.junit.Before;
import org.junit.Test;

import static io.crate.testing.TestingHelpers.isSQL;
import static org.hamcrest.Matchers.allOf;

public class SingleRowSubselectAnalyzerTest extends CrateDummyClusterServiceUnitTest {

    private SQLExecutor e;

    @Before
    public void prepare() {
        e = SQLExecutor.builder(clusterService).enableDefaultTables().build();
    }

    @Test
    public void testSingleRowSubselectInWhereClause() throws Exception {
        SelectAnalyzedStatement stmt = e.analyze("select * from t1 where x = (select y from t2)");
        assertThat(stmt.relation().querySpec().where().query(),
            isSQL("(doc.t1.x = single_value(SelectSymbol{integer_table}))"));
    }

    @Test
    public void testSingleRowSubselectInWhereClauseNested() throws Exception {
        SelectAnalyzedStatement stmt = e.analyze(
            "select a from t1 where x = (select y from t2 where y = (select z from t3))");
        assertThat(stmt.relation().querySpec().where().query(),
            isSQL("(doc.t1.x = single_value(SelectSymbol{integer_table}))"));
    }

    @Test
    public void testSingleRowSubselectInSelectList() throws Exception {
        SelectAnalyzedStatement stmt = e.analyze("select (select b from t2 limit 1) from t1");
        assertThat(stmt.relation().querySpec().outputs(), isSQL("single_value(SelectSymbol{string_table})"));
    }

    @Test
    public void testSubselectWithMultipleColumns() throws Exception {
        expectedException.expectMessage("Subqueries with more than 1 column are not supported.");
        e.analyze("select (select b, b from t2 limit 1) from t1");
    }

    @Test
    public void testSingleRowSubselectInAssignmentOfUpdate() throws Exception {
        expectedException.expectMessage("Subquery not supported in this statement");
        UpdateAnalyzedStatement stmt = e.analyze("update t1 set x = (select y from t2)");
        /*
        assertThat(
            stmt.nestedStatements().get(0).assignments().values().iterator().next(),
            Matchers.instanceOf(SelectSymbol.class));
            */
    }

    @Test
    public void testSingleRowSubselectInWhereClauseOfDelete() throws Exception {
        expectedException.expectMessage("Subquery not supported in this statement");
        DeleteAnalyzedStatement stmt = e.analyze("delete from t1 where x = (select y from t2)");
        /*
        assertThat(stmt.whereClauses().get(0).query(),
            isSQL("(doc.t1.x = SelectSymbol{integer})"));
        */
    }

    @Test
    public void testSubselectInWhereIn() throws Exception {
        expectedException.expectMessage(allOf(Matchers.startsWith("Expression"), Matchers.endsWith("is not supported in IN")));
        e.analyze("select * from t1 where x in (select y from t2)");
    }

    @Test
    public void testMatchPredicateWithSingleRowSubselect() throws Exception {
        SelectAnalyzedStatement stmt = e.analyze(
            "select * from users where match(shape 1.2, (select shape from users limit 1))");
        assertThat(stmt.relation().querySpec().where().query(),
            isSQL("match({\"shape\"=1.2}, single_value(SelectSymbol{geo_shape_table}), 'intersects', {})"));
    }

    @Test
    public void testLikeSupportsSubQueries() throws Exception {
        SelectAnalyzedStatement stmt = e.analyze("select * from users where name like (select 'foo')");
        assertThat(stmt.relation().querySpec().where().query(),
            isSQL("(doc.users.name LIKE single_value(SelectSymbol{string_table}))"));
    }

    @Test
    public void testAnySupportsSubQueries() throws Exception {
        SelectAnalyzedStatement stmt = e.analyze("select * from users where (select 'bar') = ANY (tags)");
        assertThat(stmt.relation().querySpec().where().query(),
            isSQL("(single_value(SelectSymbol{string_table}) = ANY(doc.users.tags))"));

        stmt = e.analyze("select * from users where 'bar' = ANY (select 'bar')");
        assertThat(stmt.relation().querySpec().where().query(),
            isSQL("('bar' = ANY(SelectSymbol{string_table}))"));
    }
}
