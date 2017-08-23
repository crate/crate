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

package io.crate.planner.consumer;

import io.crate.action.sql.SessionContext;
import io.crate.analyze.MultiSourceSelect;
import io.crate.analyze.QueriedTableRelation;
import io.crate.analyze.SelectAnalyzedStatement;
import io.crate.analyze.relations.QueriedRelation;
import io.crate.analyze.symbol.Function;
import io.crate.analyze.symbol.SelectSymbol;
import io.crate.analyze.symbol.Symbol;
import io.crate.metadata.TransactionContext;
import io.crate.planner.Plan;
import io.crate.planner.node.dql.join.NestedLoop;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.SQLExecutor;
import io.crate.testing.T3;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

import static io.crate.testing.TestingHelpers.getFunctions;
import static io.crate.testing.TestingHelpers.isSQL;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;

public class SemiJoinsTest extends CrateDummyClusterServiceUnitTest {

    private SQLExecutor executor;
    private SemiJoins semiJoins = new SemiJoins(getFunctions());

    @Before
    public void initExecutor() throws Exception {
        executor = SQLExecutor.builder(clusterService)
            .addDocTable(T3.T1_INFO)
            .addDocTable(T3.T2_INFO)
            .addDocTable(T3.T3_INFO)
            .build();
    }

    private <T extends Symbol> T asSymbol(String expression) {
        //noinspection unchecked - for tests it's okay to do nasty casts
        return (T) executor.asSymbol(T3.SOURCES, expression);
    }

    @Test
    public void testGatherRewriteCandidatesSingle() throws Exception {
        Symbol query = asSymbol("a in (select 'foo')");
        assertThat(SemiJoins.gatherRewriteCandidates(query).size(), is(1));
    }

    @Test
    public void testGatherRewriteCandidatesTwo() throws Exception {
        Symbol query = asSymbol("a in (select 'foo') and a = 'foo' and x in (select 1)");
        List<Function> candidates = SemiJoins.gatherRewriteCandidates(query);
        assertThat(candidates.size(), is(2));

        for (Function candidate : candidates) {
            assertThat(candidate.info().ident().name(), is("any_="));
        }
    }

    @Test
    public void testMakeJoinConditionWith() throws Exception {
        SelectAnalyzedStatement stmt = executor.analyze("select * from t1 where a in (select 'foo')");
        QueriedTableRelation rel = ((QueriedTableRelation) stmt.relation());
        Function query = (Function) rel.querySpec().where().query();

        SelectSymbol subquery = SemiJoins.getSubqueryOrNull(query.arguments().get(1));
        Symbol joinCondition = SemiJoins.makeJoinCondition(query, rel.tableRelation(), subquery.relation());

        assertThat(joinCondition, isSQL("(doc.t1.a = doc.empty_row.'foo')"));
    }

    @Test
    public void testRewriteOfWhereClause() throws Exception {
        SelectAnalyzedStatement stmt = executor.analyze("select * from t1 where a in (select 'foo') and x = 10");
        QueriedRelation rel = stmt.relation();
        MultiSourceSelect semiJoin = (MultiSourceSelect) semiJoins.tryRewrite(rel, new TransactionContext(SessionContext.create()));

        assertThat(
            semiJoin.querySpec().where(),
            isSQL("WhereClause{MATCH_ALL=true}"));

        assertThat(semiJoin.joinPairs().get(0).condition(), isSQL("(doc.t1.a = doc.empty_row.'foo')"));
    }

    @Test
    public void testNotInIsNotRewritten() throws Exception {
        SelectAnalyzedStatement stmt = executor.analyze("select * from t1 where a not in (select 'foo')");
        QueriedRelation rel = stmt.relation();
        QueriedRelation semiJoin = semiJoins.tryRewrite(rel, new TransactionContext(SessionContext.create()));

        assertThat(semiJoin, nullValue());
    }

    @Test
    public void testQueryWithOrIsNotRewritten() throws Exception {
        SelectAnalyzedStatement stmt = executor.analyze("select * from t1 where a in (select 'foo') or a = '10'");
        QueriedRelation rel = stmt.relation();
        QueriedRelation semiJoin = semiJoins.tryRewrite(rel, new TransactionContext(SessionContext.create()));

        assertThat(semiJoin, nullValue());
    }

    @Test
    public void testDisabledByDefault() throws Exception {
        Plan plan = executor.plan("select * from t1 where a in (select 'foo')");
        assertThat(plan, not(instanceOf(NestedLoop.class)));
    }
}
