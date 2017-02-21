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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.crate.action.sql.SessionContext;
import io.crate.analyze.*;
import io.crate.analyze.relations.JoinPair;
import io.crate.planner.node.dql.join.JoinType;
import io.crate.sql.parser.SqlParser;
import io.crate.sql.tree.QualifiedName;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.SQLExecutor;
import io.crate.testing.T3;
import org.hamcrest.Matchers;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collection;
import java.util.Set;

import static io.crate.testing.TestingHelpers.isSQL;
import static org.hamcrest.core.Is.is;

public class ManyTableConsumerTest extends CrateDummyClusterServiceUnitTest {

    private SQLExecutor e;

    @Before
    public void prepare() {
        e = SQLExecutor.builder(clusterService).enableDefaultTables().build();
    }

    private MultiSourceSelect analyze(String statement) {
        Analysis analysis = e.analyzer.boundAnalyze(
            SqlParser.createStatement(statement), SessionContext.SYSTEM_SESSION, ParameterContext.EMPTY);
        MultiSourceSelect mss = (MultiSourceSelect) ((SelectAnalyzedStatement) analysis.analyzedStatement()).relation();
        ManyTableConsumer.replaceFieldsWithRelationColumns(mss);
        return mss;
    }

    @Test
    public void testQuerySplitting() throws Exception {
        MultiSourceSelect mss = analyze("select * from t1 " +
                                        "join t2 on t1.a = t2.b " +
                                        "join t3 on t2.b = t3.c " +
                                        "order by t1.a, t2.b, t3.c");
        TwoTableJoin root = ManyTableConsumer.buildTwoTableJoinTree(mss);
        TwoTableJoin t1AndT2 = ((TwoTableJoin) root.left().relation());

        assertThat(t1AndT2.joinPair().condition(), isSQL("(RELCOL(doc.t1, 0) = RELCOL(doc.t2, 0))"));
        assertThat(root.joinPair().condition(), isSQL("(RELCOL(join.doc.t1.doc.t2, 3) = RELCOL(doc.t3, 0))"));
    }

    @Test
    public void testQuerySplittingWithRelationReOrdering() throws Exception {
        MultiSourceSelect mss = analyze("select * from t1, t2, t3 " +
                                        "where t3.c = t2.b " +
                                        "order by t3.c");
        TwoTableJoin root = ManyTableConsumer.buildTwoTableJoinTree(mss);
        TwoTableJoin left = (TwoTableJoin) root.left().relation();

        assertThat(left.querySpec().where().query(), isSQL("(RELCOL(doc.t3, 0) = RELCOL(doc.t2, 0))"));
    }

    @Test
    public void testQuerySplittingWithBadRelationOrder() throws Exception {
        MultiSourceSelect mss = analyze("select * from t1 " +
                                        "join t2 on t1.a = t2.b " +
                                        "join t3 on t2.b = t3.c " +
                                        "order by t3.c, t1.a, t2.b");
        TwoTableJoin root = ManyTableConsumer.buildTwoTableJoinTree(mss);
        TwoTableJoin t3AndT1 = ((TwoTableJoin) root.left().relation());

        assertThat(t3AndT1.querySpec().where().query(), isSQL("null"));

        assertThat(root.joinPair().condition(), Matchers.anyOf(
            // order of the AND clauses is not deterministic, but both are okay as they're semantically the same
            isSQL("((RELCOL(doc.t2, 0) = RELCOL(join.doc.t3.doc.t1, 0)) " +
                  "AND (RELCOL(join.doc.t3.doc.t1, 2) = RELCOL(doc.t2, 0)))"),
            isSQL("((RELCOL(join.doc.t3.doc.t1, 2) = RELCOL(doc.t2, 0)) " +
                  "AND (RELCOL(doc.t2, 0) = RELCOL(join.doc.t3.doc.t1, 0)))")));
    }

    @Test
    public void testQuerySplittingReplacesCopiedSymbols() throws Exception {
        MultiSourceSelect mss = analyze("select * from t1, t2 " +
                                        "where t1.x = 1 or t2.y = 1 " +
                                        "order by t1.x + t1.x");
        TwoTableJoin root = ManyTableConsumer.buildTwoTableJoinTree(mss);
        assertThat(root.querySpec().orderBy().get().orderBySymbols(), isSQL("add(RELCOL(doc.t1, 0), RELCOL(doc.t1, 0))"));
        assertThat(root.left().querySpec().orderBy().get().orderBySymbols(), isSQL("add(doc.t1.x, doc.t1.x)"));
    }

    @Test
    public void testOptimizeJoinNoPresort() throws Exception {
        JoinPair pair1 = new JoinPair(T3.T1, T3.T2, JoinType.CROSS);
        JoinPair pair2 = new JoinPair(T3.T2, T3.T3, JoinType.CROSS);
        @SuppressWarnings("unchecked")
        Collection<QualifiedName> qualifiedNames = ManyTableConsumer.orderByJoinConditions(
            Arrays.asList(T3.T1, T3.T2, T3.T3),
            ImmutableSet.<Set<QualifiedName>>of(),
            ImmutableList.of(pair1, pair2),
            ImmutableList.<QualifiedName>of());

        assertThat(qualifiedNames, Matchers.contains(T3.T1, T3.T2, T3.T3));
    }

    @Test
    public void testOptimizeJoinWithoutJoinConditionsAndPreSort() throws Exception {
        Collection<QualifiedName> qualifiedNames = ManyTableConsumer.orderByJoinConditions(
            Arrays.asList(T3.T1, T3.T2, T3.T3),
            ImmutableSet.<Set<QualifiedName>>of(),
            ImmutableList.<JoinPair>of(),
            ImmutableList.of(T3.T2));

        assertThat(qualifiedNames, Matchers.contains(T3.T2, T3.T1, T3.T3));
    }

    @Test
    public void testNoOptimizeWithSortingAndOuterJoin() throws Exception {
        JoinPair pair1 = new JoinPair(T3.T1, T3.T2, JoinType.LEFT);
        JoinPair pair2 = new JoinPair(T3.T2, T3.T3, JoinType.LEFT);
        @SuppressWarnings("unchecked")
        Collection<QualifiedName> qualifiedNames = ManyTableConsumer.orderByJoinConditions(
            Arrays.asList(T3.T1, T3.T2, T3.T3),
            ImmutableSet.<Set<QualifiedName>>of(),
            ImmutableList.of(pair1, pair2),
            ImmutableList.of(T3.T3, T3.T2));

        assertThat(qualifiedNames, Matchers.contains(T3.T1, T3.T2, T3.T3));
    }

    @Test
    public void testSortOnOuterJoinOuterRelation() throws Exception {
        MultiSourceSelect mss = analyze("select * from t1 " +
                                        "left join t2 on t1.a = t2.b " +
                                        "order by t2.b");
        TwoTableJoin root = ManyTableConsumer.twoTableJoin(mss);

        assertThat(root.right().querySpec().orderBy().isPresent(), is(false));
        assertThat(root.querySpec().orderBy().get(), isSQL("RELCOL(doc.t2, 0)"));
    }

    @Test
    public void test3TableSortOnOuterJoinOuterRelation() throws Exception {
        MultiSourceSelect mss = analyze("select * from t1 " +
                                        "left join t2 on t1.a = t2.b " +
                                        "left join t3 on t2.b = t3.c " +
                                        "order by t2.b, t3.c");
        TwoTableJoin root = ManyTableConsumer.buildTwoTableJoinTree(mss);
        TwoTableJoin t1AndT2 = ((TwoTableJoin) root.left().relation());

        assertThat(t1AndT2.right().querySpec().orderBy().isPresent(), is(false));
        assertThat(t1AndT2.querySpec().orderBy().get(), isSQL("RELCOL(doc.t2, 0)"));

        assertThat(root.right().querySpec().orderBy().isPresent(), is(false));
        assertThat(root.querySpec().orderBy().get(), isSQL("RELCOL(join.doc.t1.doc.t2, 3), RELCOL(doc.t3, 0)"));
    }

    @Test
    public void testJoinConditionsAreRewrittenIfOutputsChanges() throws Exception {
        MultiSourceSelect mss = analyze("select t1.a from t1 " +
                                        "left join t2 on t1.a = t2.b " +
                                        "where t1.x = 1 and t2.y in (1)");
        TwoTableJoin root = ManyTableConsumer.twoTableJoin(mss);
        // if Rewriter does not operate correctly on the joinPairs, t2 RelationColumn index would be 1 instead of 0
        // 2 outputs, t2.y + t2.b on t2 are rewritten to t2.b only because whereClause outputs must not be collected
        // so index for t2.b must be shifted
        assertThat(root.joinPair().condition(), isSQL("(RELCOL(doc.t1, 0) = RELCOL(doc.t2, 0))"));
    }

    @Test
    public void test3TableSortOnWhere() throws Exception {
        MultiSourceSelect mss = analyze("select * from t1,t2,t3 " +
                                        "where t1.a=t3.c");
        TwoTableJoin root = ManyTableConsumer.buildTwoTableJoinTree(mss);
        assertThat(root.toString(), is("join.join.doc.t1.doc.t3.doc.t2"));
        TwoTableJoin t1Andt3 = ((TwoTableJoin) root.left().relation());
        assertThat(t1Andt3.toString(), is("join.doc.t1.doc.t3"));
        assertThat(root.right().qualifiedName().toString(), is("doc.t2"));
    }
}
