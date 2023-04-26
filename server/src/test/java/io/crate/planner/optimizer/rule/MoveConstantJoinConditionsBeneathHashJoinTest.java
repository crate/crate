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

import static io.crate.testing.Asserts.assertThat;

import java.util.Collections;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;

import io.crate.analyze.WhereClause;
import io.crate.analyze.relations.AbstractTableRelation;
import io.crate.analyze.relations.AnalyzedRelation;
import io.crate.metadata.CoordinatorTxnCtx;
import io.crate.metadata.RelationName;
import io.crate.planner.operators.Collect;
import io.crate.planner.operators.Filter;
import io.crate.planner.operators.HashJoin;
import io.crate.planner.optimizer.matcher.Captures;
import io.crate.planner.optimizer.matcher.Match;
import io.crate.statistics.TableStats;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.SqlExpressions;
import io.crate.testing.T3;

public class MoveConstantJoinConditionsBeneathHashJoinTest extends CrateDummyClusterServiceUnitTest {

    private SqlExpressions sqlExpressions;
    private Map<RelationName, AnalyzedRelation> sources;

    @Before
    public void prepare() throws Exception {
        sources = T3.sources(clusterService);
        sqlExpressions = new SqlExpressions(sources);
    }

    @Test
    public void test_optimize_hashjoin_when_constant_can_be_pused_down() {
        var t1 = (AbstractTableRelation<?>) sources.get(T3.T1);
        var t2 = (AbstractTableRelation<?>) sources.get(T3.T2);

        Collect c1 = new Collect(t1, Collections.emptyList(), WhereClause.MATCH_ALL, 1, 1);
        Collect c2 = new Collect(t2, Collections.emptyList(), WhereClause.MATCH_ALL, 1, 1);

        // This condition has a non-constant part `doc.t1.x = doc.t2.y` and a constant part `doc.t2.b = 'abc'`
        var joinCondition = sqlExpressions.asSymbol("doc.t1.x = doc.t2.y and doc.t2.b = 'abc'");
        var nonConstantPart = sqlExpressions.asSymbol("doc.t1.x = doc.t2.y");
        var constantPart = sqlExpressions.asSymbol("doc.t2.b = 'abc'");

        HashJoin hashJoin = new HashJoin(c1, c2, joinCondition, false);
        var rule = new MoveConstantJoinConditionsBeneathHashJoin();
        Match<HashJoin> match = rule.pattern().accept(hashJoin, Captures.empty());

        assertThat(match.isPresent()).isTrue();
        assertThat(match.value()).isEqualTo(hashJoin);

        HashJoin result = (HashJoin) rule.apply(match.value(),
                                                match.captures(),
                                                new TableStats(),
                                                CoordinatorTxnCtx.systemTransactionContext(),
                                                sqlExpressions.nodeCtx);

        assertThat(result.joinCondition()).isEqualTo(nonConstantPart);
        assertThat(result.lhs()).isEqualTo(c1);
        Filter filter = (Filter) result.rhs();
        assertThat(filter.source()).isEqualTo(c2);
        assertThat(filter.query()).isEqualTo(constantPart);
    }
}
