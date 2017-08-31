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

package io.crate.analyze.relations;

import com.google.common.collect.ImmutableList;
import io.crate.analyze.symbol.Symbol;
import io.crate.sql.tree.QualifiedName;

import javax.annotation.Nullable;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.ListIterator;
import java.util.Set;
import java.util.function.Function;

public final class JoinPairs {

    /**
     * Searches {@code pairs} for a pair of {@code left ⋈ right}
     * <ul>
     *     <li>The matching is fuzzy, so {@code right ⋈ left} also matches if the join-type supports being inverted</li>
     *     <li>If no pair is found a CROSS JOIN pair is created and returned.</li>
     *     <li>The found pair is also removed from {@code pairs}</li>
     * </ul>
     */
    public static JoinPair findAndRemovePair(List<JoinPair> pairs, QualifiedName left, QualifiedName right) {
        JoinPair joinPair = fuzzyFindPair(pairs, left, right);
        if (joinPair == null) {
            return JoinPair.crossJoin(left, right);
        }
        assert !(pairs instanceof ImmutableList) : "joinPairs list must be mutable if it contains items";
        pairs.remove(joinPair);
        return joinPair;
    }

    /**
     * Finds a JoinPair within pair where pair.left = lhs and pair.right = rhs
     */
    @Nullable
    public static JoinPair exactFindPair(Collection<JoinPair> pairs, QualifiedName lhs, QualifiedName rhs) {
        for (JoinPair pair : pairs) {
            if (pair.equalsNames(lhs, rhs)) {
                return pair;
            }
        }
        return null;
    }

    /**
     * Finds a JoinPair within pairs where either
     *
     *  - pair.left = lhs and pair.right = rhs
     *  - or pair.left = rhs and pair.right = lhs, in which case a reversed pair is returned (and the original pair in the list is replaced)
     */
    @Nullable
    public static JoinPair fuzzyFindPair(List<JoinPair> pairs, QualifiedName lhs, QualifiedName rhs) {
        JoinPair exactMatch = exactFindPair(pairs, lhs, rhs);
        if (exactMatch == null) {
            ListIterator<JoinPair> it = pairs.listIterator();
            while (it.hasNext()) {
                JoinPair pair = it.next();
                if (pair.equalsNames(rhs, lhs)) {
                    JoinPair reversed = pair.reverse();
                    it.set(reversed); // change list entry so that the found entry can be removed from pairs
                    return reversed;
                }
            }
        }
        return exactMatch;
    }

    /**
     * Returns all relation names which are part of an outer join.
     */
    public static Set<QualifiedName> outerJoinRelations(List<JoinPair> joinPairs) {
        Set<QualifiedName> outerJoinRelations = new HashSet<>();
        for (JoinPair joinPair : joinPairs) {
            if (joinPair.joinType().isOuter()) {
                outerJoinRelations.add(joinPair.left());
                outerJoinRelations.add(joinPair.right());
            }
        }
        return outerJoinRelations;
    }

    /**
     * Rewrite names of matching join pair relations and inside the condition function
     */
    public static void rewriteNames(QualifiedName left,
                                    QualifiedName right,
                                    QualifiedName newName,
                                    Function<? super Symbol, ? extends Symbol> replaceFunction,
                                    List<JoinPair> joinPairs) {
        for (JoinPair joinPair : joinPairs) {
            joinPair.replaceNames(left, right, newName);
            joinPair.replaceCondition(replaceFunction);
        }
    }

    /**
     * Returns true if relation name is part of an outer join and on the outer side.
     */
    static boolean isOuterRelation(QualifiedName name, List<JoinPair> joinPairs) {
        for (JoinPair joinPair : joinPairs) {
            if (joinPair.isOuterRelation(name)) {
                return true;
            }
        }
        return false;
    }
}
