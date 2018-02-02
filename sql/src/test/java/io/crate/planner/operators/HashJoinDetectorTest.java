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

package io.crate.planner.operators;

import io.crate.analyze.relations.JoinPair;
import io.crate.expression.symbol.Symbol;
import io.crate.planner.node.dql.join.JoinType;
import io.crate.test.integration.CrateUnitTest;
import io.crate.testing.SqlExpressions;
import io.crate.testing.T3;
import org.junit.Test;

import static org.hamcrest.Matchers.is;

public class HashJoinDetectorTest extends CrateUnitTest {

    private static final SqlExpressions SQL_EXPRESSIONS = new SqlExpressions(T3.SOURCES);

    @Test
    public void testPossibleOnInnerContainingEqCondition() {
        Symbol joinCondition = SQL_EXPRESSIONS.asSymbol("t1.x = t2.y");
        JoinPair joinPair = JoinPair.of(T3.T1, T3.T2, JoinType.INNER, joinCondition);
        assertThat(HashJoinDetector.isHashJoinPossible(joinPair), is(true));
    }

    @Test
    public void testPossibleOnInnerContainingEqAndAnyCondition() {
        Symbol joinCondition = SQL_EXPRESSIONS.asSymbol("t1.x > t2.y and t1.a = t2.b and not(t1.i = t2.i)");
        JoinPair joinPair = JoinPair.of(T3.T1, T3.T2, JoinType.INNER, joinCondition);
        assertThat(HashJoinDetector.isHashJoinPossible(joinPair), is(true));
    }

    @Test
    public void testNotPossibleOnInnerWithoutAnyEqCondition() {
        Symbol joinCondition = SQL_EXPRESSIONS.asSymbol("t1.x > t2.y and t1.a > t2.b");
        JoinPair joinPair = JoinPair.of(T3.T1, T3.T2, JoinType.INNER, joinCondition);
        assertThat(HashJoinDetector.isHashJoinPossible(joinPair), is(false));
    }

    @Test
    public void testNotPossibleOnInnerContainingEqOrAnyCondition() {
        Symbol joinCondition = SQL_EXPRESSIONS.asSymbol("t1.x = t2.y and t1.a = t2.b or t1.i = t2.i");
        JoinPair joinPair = JoinPair.of(T3.T1, T3.T2, JoinType.INNER, joinCondition);
        assertThat(HashJoinDetector.isHashJoinPossible(joinPair), is(false));
    }

    @Test
    public void testNotPossibleIfNotAnInnerJoin() {
        JoinPair joinPair = JoinPair.of(T3.T1, T3.T2, JoinType.CROSS, null);
        assertThat(HashJoinDetector.isHashJoinPossible(joinPair), is(false));

        Symbol joinCondition = SQL_EXPRESSIONS.asSymbol("t1.x = t2.y");
        joinPair = JoinPair.of(T3.T1, T3.T2, JoinType.LEFT, joinCondition);
        assertThat(HashJoinDetector.isHashJoinPossible(joinPair), is(false));

        joinPair = JoinPair.of(T3.T1, T3.T2, JoinType.RIGHT, joinCondition);
        assertThat(HashJoinDetector.isHashJoinPossible(joinPair), is(false));

        joinPair = JoinPair.of(T3.T1, T3.T2, JoinType.FULL, joinCondition);
        assertThat(HashJoinDetector.isHashJoinPossible(joinPair), is(false));

        joinPair = JoinPair.of(T3.T1, T3.T2, JoinType.ANTI, joinCondition);
        assertThat(HashJoinDetector.isHashJoinPossible(joinPair), is(false));

        joinPair = JoinPair.of(T3.T1, T3.T2, JoinType.SEMI, joinCondition);
        assertThat(HashJoinDetector.isHashJoinPossible(joinPair), is(false));
    }
}
