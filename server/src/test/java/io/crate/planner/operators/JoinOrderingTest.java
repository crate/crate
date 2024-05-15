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

import static org.assertj.core.api.Assertions.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.junit.Assert.assertThat;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import org.junit.Test;

import com.carrotsearch.hppc.ObjectIntHashMap;

import io.crate.metadata.RelationName;
import io.crate.testing.T3;

public class JoinOrderingTest {

    @Test
    public void testFindFirstJoinPair() {
        // SELECT * FROM t1, t2, t3 WHERE t1.id = t2.id AND t2.id = t3.id AND t3.id = t4.id
        ObjectIntHashMap<RelationName> occurrences = new ObjectIntHashMap<>(4);
        occurrences.put(T3.T1, 1);
        occurrences.put(T3.T2, 1);
        occurrences.put(T3.T3, 3);
        occurrences.put(T3.T4, 1);
        ArrayList<Set<RelationName>> joinPairs = new ArrayList<>();
        joinPairs.add(Set.of(T3.T1, T3.T2));
        joinPairs.add(Set.of(T3.T2, T3.T3));
        joinPairs.add(Set.of(T3.T3, T3.T4));
        assertThat(JoinOrdering.findAndRemoveFirstJoinPair(occurrences, joinPairs)).isEqualTo(Set.of(T3.T2, T3.T3));
    }

    @Test
    public void testFindFirstJoinPairOnlyOneOccurrence() {
        // SELECT * FROM t1, t2, t3 WHERE t1.id = t2.id AND t3.id = t4.id
        ObjectIntHashMap<RelationName> occurrences = new ObjectIntHashMap<>(4);
        occurrences.put(T3.T1, 1);
        occurrences.put(T3.T2, 1);
        occurrences.put(T3.T3, 1);
        occurrences.put(T3.T4, 1);
        Set<Set<RelationName>> sets = new LinkedHashSet<>();
        sets.add(Set.of(T3.T1, T3.T2));
        sets.add(Set.of(T3.T2, T3.T3));
        sets.add(Set.of(T3.T3, T3.T4));
        assertThat(JoinOrdering.findAndRemoveFirstJoinPair(occurrences, sets)).isEqualTo(Set.of(T3.T1, T3.T2));
    }

    @Test
    public void testOptimizeJoinNoPresort() throws Exception {
        Collection<RelationName> qualifiedNames = JoinOrdering.orderByJoinConditions(
            Arrays.asList(T3.T1, T3.T2, T3.T3),
            Set.of(new LinkedHashSet<>(List.of(T3.T1, T3.T2))),
            Set.of(new LinkedHashSet<>(List.of(T3.T2, T3.T3)))
        );
        assertThat(qualifiedNames, contains(T3.T1, T3.T2, T3.T3));
    }
}
