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

package io.crate.planner.operators;

import io.crate.expression.symbol.Symbol;
import io.crate.planner.node.dql.join.JoinType;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.SqlExpressions;
import io.crate.testing.T3;
import org.junit.Before;
import org.junit.Test;

import static org.hamcrest.Matchers.is;

public class EquiJoinDetectorTest extends CrateDummyClusterServiceUnitTest {

    private SqlExpressions sqlExpressions;

    @Before
    public void prepare() throws Exception {
        sqlExpressions = new SqlExpressions(T3.sources(clusterService));
    }

    @Test
    public void testPossibleOnInnerContainingEqCondition() {
        Symbol joinCondition = sqlExpressions.asSymbol("t1.x = t2.y");
        assertThat(EquiJoinDetector.isHashJoinPossible(JoinType.INNER, joinCondition), is(true));
    }

    @Test
    public void testPossibleOnInnerContainingEqAndAnyCondition() {
        Symbol joinCondition = sqlExpressions.asSymbol("t1.x > t2.y and t1.a = t2.b and not(t1.i = t2.i)");
        assertThat(EquiJoinDetector.isHashJoinPossible(JoinType.INNER, joinCondition), is(true));
    }

    @Test
    public void testNotPossibleOnInnerWithoutAnyEqCondition() {
        Symbol joinCondition = sqlExpressions.asSymbol("t1.x > t2.y and t1.a > t2.b");
        assertThat(EquiJoinDetector.isHashJoinPossible(JoinType.INNER, joinCondition), is(false));
    }

    @Test
    public void testPossibleOnInnerWithEqAndScalarOnOneRelation() {
        Symbol joinCondition = sqlExpressions.asSymbol("t1.x + t1.i = t2.b");
        assertThat(EquiJoinDetector.isHashJoinPossible(JoinType.INNER, joinCondition), is(true));
    }

    @Test
    public void testNotPossibleOnInnerWithEqAndScalarOnMultipleRelations() {
        Symbol joinCondition = sqlExpressions.asSymbol("t1.x + t2.y = 4");
        assertThat(EquiJoinDetector.isHashJoinPossible(JoinType.INNER, joinCondition), is(false));
    }

    @Test
    public void testNotPossibleOnInnerContainingEqOrAnyCondition() {
        Symbol joinCondition = sqlExpressions.asSymbol("t1.x = t2.y and t1.a = t2.b or t1.i = t2.i");
        assertThat(EquiJoinDetector.isHashJoinPossible(JoinType.INNER, joinCondition), is(false));
    }

    @Test
    public void testNotPossibleIfNotAnInnerJoin() {
        assertThat(EquiJoinDetector.isHashJoinPossible(JoinType.CROSS, null), is(false));

        Symbol joinCondition = sqlExpressions.asSymbol("t1.x = t2.y");
        assertThat(EquiJoinDetector.isHashJoinPossible(JoinType.LEFT, joinCondition), is(false));
        assertThat(EquiJoinDetector.isHashJoinPossible(JoinType.RIGHT, joinCondition), is(false));
        assertThat(EquiJoinDetector.isHashJoinPossible(JoinType.FULL, joinCondition), is(false));
        assertThat(EquiJoinDetector.isHashJoinPossible(JoinType.ANTI, joinCondition), is(false));
        assertThat(EquiJoinDetector.isHashJoinPossible(JoinType.SEMI, joinCondition), is(false));
    }

    @Test
    public void testNotPossibleOnEqWithoutRelationFieldsOnBothSides() {
        Symbol joinCondition = sqlExpressions.asSymbol("t1.x = 4");
        assertThat(EquiJoinDetector.isHashJoinPossible(JoinType.INNER, joinCondition), is(false));
    }

    @Test
    public void testNotPossibleOnNotWrappingEq() {
        Symbol joinCondition = sqlExpressions.asSymbol("NOT (t1.a = t2.b)");
        assertThat(EquiJoinDetector.isHashJoinPossible(JoinType.INNER, joinCondition), is(false));
    }
}
