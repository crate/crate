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
import io.crate.analyze.QueriedTable;
import io.crate.analyze.relations.AnalyzedRelation;
import io.crate.expression.symbol.Function;
import io.crate.expression.symbol.SelectSymbol;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.CoordinatorTxnCtx;
import io.crate.planner.node.dql.join.JoinType;
import io.crate.planner.operators.LogicalPlan;
import io.crate.sql.tree.QualifiedName;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.SQLExecutor;
import io.crate.testing.T3;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static io.crate.planner.operators.LogicalPlannerTest.isPlan;
import static io.crate.testing.T3.T1;
import static io.crate.testing.TestingHelpers.getFunctions;
import static io.crate.testing.TestingHelpers.isSQL;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

public class SemiJoinsTest extends CrateDummyClusterServiceUnitTest {

    private SQLExecutor executor;
    private SemiJoins semiJoins = new SemiJoins(getFunctions());
    private Map<QualifiedName, AnalyzedRelation> sources;

    @Before
    public void initExecutor() throws Exception {
        executor = SQLExecutor.builder(clusterService)
            .addTable(T3.T1_DEFINITION)
            .addTable(T3.T2_DEFINITION)
            .addTable(T3.T3_DEFINITION)
            .build();
        sources = T3.sources(clusterService);
    }

    private <T extends Symbol> T asSymbol(String expression) {
        //noinspection unchecked - for tests it's okay to do nasty casts
        return (T) executor.asSymbol(sources, expression);
    }

    @Test
    public void testGatherRewriteCandidatesSingle() throws Exception {
        Symbol query = asSymbol("a in (select 'foo')");
        assertThat(SemiJoins.gatherRewriteCandidates(query).size(), is(1));
    }

    @Test
    public void testGatherRewriteCandidatesTwo() throws Exception {
        Symbol query = asSymbol("a in (select 'foo') and a = 'foo' and x not in (select 1)");
        List<SemiJoins.Candidate> candidates = SemiJoins.gatherRewriteCandidates(query);
        assertThat(candidates.size(), is(2));

        assertThat(candidates.get(0).joinType, is(JoinType.SEMI));
        assertThat(candidates.get(0).function.info().ident().name(), is("any_="));
        assertThat(candidates.get(1).joinType, is(JoinType.ANTI));
        assertThat(candidates.get(1).function.info().ident().name(), is("op_not"));
    }

    @Test
    public void testMakeJoinConditionWith() throws Exception {
        QueriedTable relation = executor.analyze("select * from t1 where a in (select 'foo')");
        Function query = (Function) relation.querySpec().where().query();

        SelectSymbol subquery = SemiJoins.getSubqueryOrNull(query.arguments().get(1));
        Symbol joinCondition = SemiJoins.makeJoinCondition(
            new SemiJoins.SemiJoinCandidate(query, subquery),
            relation.tableRelation());

        assertThat(joinCondition, isSQL("(doc.t1.a = 'foo')"));
    }

    @Test
    public void testSemiJoinRewriteOfWhereClause() throws Exception {
        AnalyzedRelation rel = executor.analyze("select * from t1 where a in (select 'foo') and x = 10");
        MultiSourceSelect semiJoin = (MultiSourceSelect) semiJoins.tryRewrite(
            rel, new CoordinatorTxnCtx(SessionContext.systemSessionContext()));

        assertThat(semiJoin.querySpec().where(), isSQL("true"));
        assertThat(semiJoin.sources().get(QualifiedName.of(T1.toString())).querySpec(),
            isSQL("SELECT doc.t1.a, doc.t1.x, doc.t1.i WHERE (doc.t1.x = 10)"));
        assertThat(semiJoin.sources().get(new QualifiedName(Arrays.asList("S0", "", "empty_row")))
            .querySpec(), isSQL("SELECT 'foo'"));

        assertThat(semiJoin.joinPairs().get(0).condition(), isSQL("(doc.t1.a = S0..empty_row.'foo')"));
        assertThat(semiJoin.joinPairs().get(0).joinType(), is(JoinType.SEMI));
    }

    @Test
    public void testAntiJoinRewriteOfWhereClause() throws Exception {
        AnalyzedRelation rel = executor.analyze("select * from t1 where a not in (select 'foo') and x = 10");
        MultiSourceSelect antiJoin = (MultiSourceSelect) semiJoins.tryRewrite(
            rel, new CoordinatorTxnCtx(SessionContext.systemSessionContext()));

        assertThat(antiJoin.querySpec().where(), isSQL("true"));
        assertThat(antiJoin.sources().get(QualifiedName.of(T1.toString())).querySpec(),
            isSQL("SELECT doc.t1.a, doc.t1.x, doc.t1.i WHERE (doc.t1.x = 10)"));
        assertThat(antiJoin.sources().get(new QualifiedName(Arrays.asList("S0", "", "empty_row")))
            .querySpec(), isSQL("SELECT 'foo'"));

        assertThat(antiJoin.joinPairs().get(0).condition(), isSQL("(doc.t1.a = S0..empty_row.'foo')"));
        assertThat(antiJoin.joinPairs().get(0).joinType(), is(JoinType.ANTI));
    }

    @Test
    public void testQueryWithOrIsNotRewritten() throws Exception {
        AnalyzedRelation relation = executor.analyze("select * from t1 where a in (select 'foo') or a = '10'");
        AnalyzedRelation semiJoin = semiJoins.tryRewrite(relation, new CoordinatorTxnCtx(SessionContext.systemSessionContext()));

        assertThat(semiJoin, nullValue());
    }

    @Test
    public void testDisabledByDefault() throws Exception {
        LogicalPlan logicalPlan = executor.logicalPlan("select * from t1 where a in (select 'foo')");
        assertThat(logicalPlan, isPlan(getFunctions(),
            "RootBoundary[a, x, i]\n" +
            "MultiPhase[\n" +
            "    subQueries[\n" +
            "        RootBoundary['foo']\n" +
            "        OrderBy['foo' ASC]\n" +
            "        Collect[.empty_row | ['foo'] | All]\n" +
            "    ]\n" +
            "    FetchOrEval[a, x, i]\n" +
            "    Collect[doc.t1 | [_fetchid] | (a = ANY(SelectSymbol{text_array}))]\n" +
            "]\n"));
    }

    @Test
    public void testWriteWithMultipleInClauses() throws Exception {
        AnalyzedRelation relation = executor.analyze("select * from t1 " +
                                                     "where " +
                                                     "   x in (select * from unnest([1, 2])) " +
                                                     "   and x not in (select 1)");
        AnalyzedRelation semiJoins = this.semiJoins.tryRewrite(relation, new CoordinatorTxnCtx(SessionContext.systemSessionContext()));

        assertThat(semiJoins, instanceOf(MultiSourceSelect.class));
        MultiSourceSelect mss = (MultiSourceSelect) semiJoins;
        assertThat(mss.sources().size(), is(3));
        assertThat(mss.joinPairs().get(0).joinType(), is(JoinType.SEMI));
        assertThat(mss.joinPairs().get(1).joinType(), is(JoinType.ANTI));
    }
}
