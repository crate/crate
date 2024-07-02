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

import static io.crate.testing.Asserts.assertList;
import static io.crate.testing.Asserts.assertThat;
import static io.crate.testing.Asserts.exactlyInstanceOf;
import static io.crate.testing.Asserts.isLiteral;
import static io.crate.testing.Asserts.isReference;
import static io.crate.testing.Asserts.toCondition;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;

import org.junit.Before;
import org.junit.Test;

import io.crate.analyze.relations.AnalyzedRelation;
import io.crate.expression.operator.EqOperator;
import io.crate.expression.symbol.MatchPredicate;
import io.crate.expression.symbol.SelectSymbol;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.SQLExecutor;
import io.crate.testing.T3;

public class SingleRowSubselectAnalyzerTest extends CrateDummyClusterServiceUnitTest {

    private SQLExecutor e;

    @Before
    public void prepare() throws IOException {
        e = SQLExecutor.of(clusterService)
            .addTable(TableDefinitions.USER_TABLE_DEFINITION)
            .addTable(T3.T1_DEFINITION)
            .addTable(T3.T2_DEFINITION)
            .addTable(T3.T3_DEFINITION);
    }

    @Test
    public void testSingleRowSubselectInWhereClause() throws Exception {
        QueriedSelectRelation relation = e.analyze("select * from t1 where x = (select y from t2)");
        assertThat(relation.where())
            .isSQL("(doc.t1.x = (SELECT y FROM (doc.t2)))");
    }

    @Test
    public void testSingleRowSubselectInWhereClauseNested() throws Exception {
        QueriedSelectRelation relation = e.analyze(
            "select a from t1 where x = (select y from t2 where y = (select z from t3))");
        assertThat(relation.where())
            .isSQL("(doc.t1.x = (SELECT y FROM (doc.t2)))");
    }

    @Test
    public void testSingleRowSubselectInSelectList() {
        AnalyzedRelation relation = e.analyze("select (select b from t2 limit 1) from t1");
        assertList(relation.outputs()).isSQL("(SELECT b FROM (doc.t2))");
    }

    @Test
    public void testSubselectWithMultipleColumns() throws Exception {
        assertThatThrownBy(() -> e.analyze("select (select b, b from t2 limit 1) from t1"))
            .isExactlyInstanceOf(UnsupportedOperationException.class)
            .hasMessage("Subqueries with more than 1 column are not supported.");
    }

    @Test
    public void testSingleRowSubselectInAssignmentOfUpdate() throws Exception {
        AnalyzedUpdateStatement stmt = e.analyze("update t1 set x = (select y from t2)");
        assertThat(stmt.assignmentByTargetCol().values().iterator().next()).isExactlyInstanceOf(SelectSymbol.class);
    }

    @Test
    public void testSingleRowSubselectInWhereClauseOfDelete() throws Exception {
        AnalyzedDeleteStatement delete = e.analyze("delete from t1 where x = (select y from t2)");
        assertThat(delete.query())
            .isFunction(EqOperator.NAME, isReference("x"), exactlyInstanceOf(SelectSymbol.class));
    }

    @Test
    public void testMatchPredicateWithSingleRowSubselect() throws Exception {
        QueriedSelectRelation relation = e.analyze(
            "select * from users where match(shape 1.2, (select shape from users limit 1))");
        assertThat(relation.where()).isExactlyInstanceOf(MatchPredicate.class);
        MatchPredicate match = (MatchPredicate) relation.where();
        assertThat(match.identBoostMap()).hasEntrySatisfying(
            toCondition(isReference("shape")), toCondition(isLiteral(1.2)));
        assertThat(match.queryTerm()).isExactlyInstanceOf(SelectSymbol.class);
        assertThat(match.matchType()).isEqualTo("intersects");
    }

    @Test
    public void testLikeSupportsSubQueries() {
        QueriedSelectRelation relation = e.analyze("select * from users where name like (select 'foo')");
        assertThat(relation.where())
            .isSQL("(doc.users.name LIKE (SELECT 'foo' FROM (empty_row)))");
    }

    @Test
    public void testAnySupportsSubQueries() {
        QueriedSelectRelation relation = e.analyze("select * from users where (select 'bar') = ANY (tags)");
        assertThat(relation.where())
            .isSQL("((SELECT 'bar' FROM (empty_row)) = ANY(doc.users.tags))");

        relation = e.analyze("select * from users where 'bar' = ANY (select 'bar')");
        assertThat(relation.where())
            .isSQL("('bar' = ANY((SELECT 'bar' FROM (empty_row))))");
    }
}
