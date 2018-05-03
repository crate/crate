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

package io.crate.execution.engine.join;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import io.crate.analyze.relations.AnalyzedRelation;
import io.crate.analyze.relations.JoinPair;
import io.crate.expression.symbol.Symbol;
import io.crate.planner.node.dql.join.JoinType;
import io.crate.sql.tree.QualifiedName;
import io.crate.test.integration.CrateUnitTest;
import io.crate.testing.SqlExpressions;
import io.crate.testing.T3;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static io.crate.testing.TestingHelpers.isSQL;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.is;

public class JoinOperationsTest extends CrateUnitTest {

    private static final Map<QualifiedName, AnalyzedRelation> sources = ImmutableMap.of(
        new QualifiedName(T3.T1_INFO.ident().name()), T3.TR_1,
        new QualifiedName(T3.T2_INFO.ident().name()), T3.TR_2,
        new QualifiedName(T3.T3_INFO.ident().name()), T3.TR_3
    );

    private static final SqlExpressions expressions = new SqlExpressions(sources);

    private Symbol asSymbol(String expression) {
        return expressions.asSymbol(expression);
    }

    @Test
    public void testImplicitToExplicit_NoRemainingWhereQuery_NoConversion() {
        List<JoinPair> joinPairs = new ArrayList<>();
        joinPairs.add(JoinPair.of(T3.T1, T3.T2, JoinType.INNER, asSymbol("t1.a = t2.b")));
        List<JoinPair> newJoinPairs =
            JoinOperations.convertImplicitJoinConditionsToJoinPairs(joinPairs, Collections.emptyMap());

        assertThat(newJoinPairs, contains(JoinPair.of(T3.T1, T3.T2, JoinType.INNER, asSymbol("t1.a = t2.b"))));
    }

    @Test
    public void testImplicitToExplicit_QueryDoesNotInvolveTwoRelations_NoConversion() {
        List<JoinPair> joinPairs = new ArrayList<>();
        joinPairs.add(JoinPair.of(T3.T1, T3.T2, JoinType.INNER, asSymbol("t1.a = t2.b")));
        Map<Set<QualifiedName>, Symbol> remainingQueries = new HashMap<>();
        remainingQueries.put(Sets.newHashSet(T3.T1, T3.T2, T3.T3), asSymbol("t1.x = t2.y + t3.z"));
        List<JoinPair> newJoinPairs =
            JoinOperations.convertImplicitJoinConditionsToJoinPairs(joinPairs, remainingQueries);

        assertThat(newJoinPairs.size(), is(1));
        JoinPair joinPair = newJoinPairs.get(0);
        assertThat(joinPair.condition(), isSQL("(doc.t1.a = doc.t2.b)"));
        assertThat(joinPair.joinType(), is(JoinType.INNER));
        assertThat(remainingQueries.size(), is(1));
    }

    @Test
    public void testImplicitToExplicit_InnerJoinPairWithConditionAlreadyExists() {
        List<JoinPair> joinPairs = new ArrayList<>();
        joinPairs.add(JoinPair.of(T3.T1, T3.T2, JoinType.INNER, asSymbol("t1.a = t2.b")));
        Map<Set<QualifiedName>, Symbol> remainingQueries = new HashMap<>();
        remainingQueries.put(Sets.newHashSet(T3.T1, T3.T2), asSymbol("t1.x = t2.y"));
        List<JoinPair> newJoinPairs =
            JoinOperations.convertImplicitJoinConditionsToJoinPairs(joinPairs, remainingQueries);

        assertThat(newJoinPairs.size(), is(1));
        JoinPair joinPair = newJoinPairs.get(0);
        assertThat(joinPair.condition(), isSQL("((doc.t1.a = doc.t2.b) AND (doc.t1.x = doc.t2.y))"));
        assertThat(joinPair.joinType(), is(JoinType.INNER));
        assertThat(remainingQueries.isEmpty(), is(true));
    }

    @Test
    public void testImplicitToExplicit_CrossJoinPairAlreadyExists() {
        List<JoinPair> joinPairs = new ArrayList<>();
        joinPairs.add(JoinPair.of(T3.T1, T3.T2, JoinType.CROSS, null));
        Map<Set<QualifiedName>, Symbol> remainingQueries = new HashMap<>();
        remainingQueries.put(Sets.newHashSet(T3.T1, T3.T2), asSymbol("t1.x = t2.y"));
        List<JoinPair> newJoinPairs =
            JoinOperations.convertImplicitJoinConditionsToJoinPairs(joinPairs, remainingQueries);

        assertThat(newJoinPairs.size(), is(1));
        JoinPair joinPair = newJoinPairs.get(0);
        assertThat(joinPair.condition(), isSQL("(doc.t1.x = doc.t2.y)"));
        assertThat(joinPair.joinType(), is(JoinType.INNER));
        assertThat(remainingQueries.isEmpty(), is(true));
    }

    @Test
    public void testImplicitToExplicit_JoinPairDoesNotExist() {
        Map<Set<QualifiedName>, Symbol> remainingQueries = new HashMap<>();
        remainingQueries.put(Sets.newHashSet(T3.T1, T3.T2), asSymbol("t1.x = t2.y"));
        List<JoinPair> newJoinPairs =
            JoinOperations.convertImplicitJoinConditionsToJoinPairs(Collections.emptyList(), remainingQueries);

        assertThat(newJoinPairs.size(), is(1));
        JoinPair joinPair = newJoinPairs.get(0);
        assertThat(joinPair.condition(), isSQL("(doc.t1.x = doc.t2.y)"));
        assertThat(joinPair.joinType(), is(JoinType.INNER));
        assertThat(remainingQueries.isEmpty(), is(true));
    }

    @Test
    public void testImplicitToExplicit_OuterJoinPairExists_NoConversion() {
        List<JoinPair> joinPairs = new ArrayList<>();
        joinPairs.add(JoinPair.of(T3.T1, T3.T2, JoinType.LEFT, asSymbol("t1.a = t2.b")));
        Map<Set<QualifiedName>, Symbol> remainingQueries = new HashMap<>();
        remainingQueries.put(Sets.newHashSet(T3.T1, T3.T2), asSymbol("t1.x = t2.y"));
        List<JoinPair> newJoinPairs =
            JoinOperations.convertImplicitJoinConditionsToJoinPairs(joinPairs, remainingQueries);

        assertThat(newJoinPairs.size(), is(1));
        JoinPair joinPair = newJoinPairs.get(0);
        assertThat(joinPair.condition(), isSQL("(doc.t1.a = doc.t2.b)"));
        assertThat(joinPair.joinType(), is(JoinType.LEFT));
        assertThat(remainingQueries.size(), is(1));
    }

    @Test
    public void testImplicitToExplicit_SemiJoinPairExists_NoConversion() {
        List<JoinPair> joinPairs = new ArrayList<>();
        joinPairs.add(JoinPair.of(T3.T1, T3.T2, JoinType.SEMI, asSymbol("t1.a = t2.b")));
        Map<Set<QualifiedName>, Symbol> remainingQueries = new HashMap<>();
        remainingQueries.put(Sets.newHashSet(T3.T1, T3.T2), asSymbol("t1.x = t2.y"));

        List<JoinPair> newJoinPairs = JoinOperations.convertImplicitJoinConditionsToJoinPairs(joinPairs, remainingQueries);

        assertThat(newJoinPairs.size(), is(1));
        JoinPair joinPair = newJoinPairs.get(0);
        assertThat(joinPair.condition(), isSQL("(doc.t1.a = doc.t2.b)"));
        assertThat(joinPair.joinType(), is(JoinType.SEMI));
        assertThat(remainingQueries.size(), is(1));
    }
}
