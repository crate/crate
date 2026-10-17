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

import org.junit.Before;
import org.junit.Test;

import io.crate.analyze.WhereClause;
import io.crate.analyze.relations.DocTableRelation;
import io.crate.planner.operators.Collect;
import io.crate.planner.operators.JoinPlan;
import io.crate.planner.operators.LogicalPlan;
import io.crate.planner.optimizer.matcher.Captures;
import io.crate.planner.optimizer.matcher.Match;
import io.crate.sql.tree.JoinType;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.SQLExecutor;
import io.crate.testing.T3;

public class PushdownInnerJoinConditionsTest extends CrateDummyClusterServiceUnitTest {

    private SQLExecutor e;
    private Collect c1;
    private Collect c2;
    private Collect c3;

    @Before
    public void prepare() throws Exception {
        e = SQLExecutor.of(clusterService)
            .addTable(T3.T1_DEFINITION)
            .addTable(T3.T2_DEFINITION)
            .addTable(T3.T3_DEFINITION);

        c1 = new Collect(new DocTableRelation(e.schemas().getTableInfo(T3.T1)), Collections.emptyList(), WhereClause.MATCH_ALL);
        c2 = new Collect(new DocTableRelation(e.schemas().getTableInfo(T3.T2)), Collections.emptyList(), WhereClause.MATCH_ALL);
        c3 = new Collect(new DocTableRelation(e.schemas().getTableInfo(T3.T3)), Collections.emptyList(), WhereClause.MATCH_ALL);
    }

    @Test
    public void test_condition_referencing_only_rhs_is_pushed_to_rhs() {
        var rhs = new JoinPlan(c2, c3, JoinType.CROSS, null);
        var joinCondition = e.asSymbol("doc.t1.x = doc.t2.y and doc.t2.y = doc.t3.z");
        var join = new JoinPlan(c1, rhs, JoinType.INNER, joinCondition);

        var rule = new PushdownInnerJoinConditions();
        Match<JoinPlan> match = rule.pattern().accept(join, Captures.empty());

        assertThat(match.isPresent()).isTrue();
        assertThat(match.value()).isEqualTo(join);

        LogicalPlan result = rule.apply(match.value(), match.captures(), e.ruleContext());

        assertThat(result).hasOperators(
            "Join[INNER | (x = y)]",
            "  ├ Collect[doc.t1 | [] | true]",
            "  └ Filter[(y = z)]",
            "    └ Join[CROSS]",
            "      ├ Collect[doc.t2 | [] | true]",
            "      └ Collect[doc.t3 | [] | true]"
        );
    }
}
