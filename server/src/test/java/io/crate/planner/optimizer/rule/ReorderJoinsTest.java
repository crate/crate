/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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

package io.crate.planner.optimizer.rule;

import static org.junit.Assert.assertThat;

import java.util.Map;
import java.util.function.Function;
import java.util.function.IntSupplier;

import org.hamcrest.Matchers;
import org.junit.Before;
import org.junit.Test;

import io.crate.analyze.WhereClause;
import io.crate.analyze.relations.AbstractTableRelation;
import io.crate.analyze.relations.AnalyzedRelation;
import io.crate.common.collections.Lists2;
import io.crate.metadata.CoordinatorTxnCtx;
import io.crate.metadata.RelationName;
import io.crate.planner.operators.Collect;
import io.crate.planner.operators.JoinPlan;
import io.crate.planner.optimizer.costs.PlanStats;
import io.crate.planner.optimizer.matcher.Captures;
import io.crate.planner.optimizer.matcher.Match;
import io.crate.sql.tree.JoinType;
import io.crate.statistics.TableStats;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.SqlExpressions;
import io.crate.testing.T3;

public class ReorderJoinsTest extends CrateDummyClusterServiceUnitTest {

    private SqlExpressions sqlExpressions;
    private Map<RelationName, AnalyzedRelation> sources;
    private PlanStats planStats;
    private int id = 0;
    private IntSupplier ids = () -> id++;


    @Before
    public void prepare() throws Exception {
        sources = T3.sources(clusterService);
        sqlExpressions = new SqlExpressions(sources);
        planStats = new PlanStats(sqlExpressions.nodeCtx, CoordinatorTxnCtx.systemTransactionContext(), new TableStats());
    }
    @Test
    public void test() {
        var t1 = (AbstractTableRelation<?>) sources.get(T3.T1);
        var t2 = (AbstractTableRelation<?>) sources.get(T3.T2);
        var t3 = (AbstractTableRelation<?>) sources.get(T3.T3);

        Collect c1 = new Collect(1, t1, t1.outputs(), WhereClause.MATCH_ALL);
        Collect c2 = new Collect(2, t2, t2.outputs(), WhereClause.MATCH_ALL);
        Collect c3 = new Collect(3, t3, t3.outputs(), WhereClause.MATCH_ALL);

        var outputs = Lists2.concat(c1.outputs(), c2.outputs());

        JoinPlan join1 = new JoinPlan(4, outputs, c1, c2, null, JoinType.INNER, false);

        var condition2 = sqlExpressions.asSymbol("doc.t2.y = doc.t3.z");
        var outputs2 = Lists2.concat(outputs, c3.outputs());
        JoinPlan join2 = new JoinPlan(2, outputs2, join1, c3, condition2, JoinType.INNER, false);

        var rule = new ReorderJoins();
        Match<JoinPlan> match = rule.pattern().accept(join2, Captures.empty());

        assertThat(match.isPresent(), Matchers.is(true));
        assertThat(match.value(), Matchers.is(join2));

        var result = rule.apply(match.value(),
                                match.captures(),
                                planStats,
                                CoordinatorTxnCtx.systemTransactionContext(),
                                sqlExpressions.nodeCtx,
                                ids,
                                Function.identity());

        System.out.println("result = " + result);
    }
}
