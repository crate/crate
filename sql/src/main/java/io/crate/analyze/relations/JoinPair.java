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

import com.google.common.base.Function;
import com.google.common.base.Objects;
import io.crate.analyze.symbol.Symbol;
import io.crate.operation.operator.AndOperator;
import io.crate.planner.node.dql.join.JoinType;
import io.crate.sql.tree.QualifiedName;

import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class JoinPair {

    private final JoinType joinType;

    private QualifiedName left;
    private QualifiedName right;
    @Nullable
    private Symbol condition;

    public JoinPair(QualifiedName left, QualifiedName right, JoinType joinType) {
        this(left, right, joinType, null);
    }

    public JoinPair(QualifiedName left, QualifiedName right, JoinType joinType, @Nullable Symbol condition) {
        this.left = left;
        this.right = right;
        this.joinType = joinType;
        this.condition = condition;
    }

    public QualifiedName left() {
        return left;
    }

    public QualifiedName right() {
        return right;
    }

    public JoinType joinType() {
        return joinType;
    }

    @Nullable
    public Symbol condition() {
        return condition;
    }

    public void condition(Symbol condition) {
        this.condition = condition;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        JoinPair joinPair = (JoinPair) o;
        return Objects.equal(left, joinPair.left) &&
               Objects.equal(right, joinPair.right) &&
               joinType == joinPair.joinType &&
               Objects.equal(condition, joinPair.condition);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(left, right, joinType, condition);
    }

    private boolean equalsNames(QualifiedName left, QualifiedName right) {
        return this.left.equals(left) && this.right.equals(right);
    }

    private void replaceNames(QualifiedName left, QualifiedName right, QualifiedName newName) {
        if (this.left.equals(left) || this.left.equals(right)) {
            this.left = newName;
        }
        if (this.right.equals(right) || this.right.equals(left)) {
            this.right = newName;
        }
    }

    public void replaceCondition(Function<? super Symbol, Symbol> replaceFunction) {
        condition = replaceFunction.apply(condition);
    }

    /**
     * Find and return a {@link JoinPair} for the given relation names, also check for reversed names.
     * If the {@link JoinType} is INNER and a pair is found for the reversed names, merge the join conditions.
     */
    public static JoinPair ofRelationsWithMergedConditions(QualifiedName left,
                                                           QualifiedName right,
                                                           List<JoinPair> joinPairs) {
        JoinPair joinPair = JoinPair.ofRelations(left, right, joinPairs, true);
        if (joinPair == null) {
            // default to cross join (or inner, doesn't matter)
            joinPair = new JoinPair(left, right, JoinType.CROSS);
        }
        // if it's an INNER, lets check for reverse relations and merge conditions
        if (joinPair.joinType() == JoinType.INNER) {
            JoinPair joinPairReverse = JoinPair.ofRelations(right, left, joinPairs, false);
            if (joinPairReverse != null) {
                joinPair.condition(AndOperator.join(Arrays.asList(joinPair.condition(), joinPairReverse.condition())));
            }
        }

        return joinPair;
    }

    /**
     * Returns the {@link JoinPair} of a pair of relation names based on the given list of join pairs.
     * If <p>checkReversePairs</p> is true, also check for any {@link JoinPair} with switched names.
     */
    @Nullable
    public static JoinPair ofRelations(QualifiedName left,
                                       QualifiedName right,
                                       List<JoinPair> joinPairs,
                                       boolean checkReversePair) {
        for (JoinPair joinPair : joinPairs) {
            if (joinPair.equalsNames(left, right)) {
                return joinPair;
            }
        }
        // check if relations were switched due to some optimization
        if (checkReversePair) {
            for (JoinPair joinPair : joinPairs) {
                if (joinPair.equalsNames(right, left)) {
                    JoinPair reverseJoinPair = new JoinPair(
                        joinPair.right,
                        joinPair.left,
                        joinPair.joinType().invert(),
                        joinPair.condition);
                    joinPairs.add(reverseJoinPair);
                    return reverseJoinPair;
                }
            }
        }

        return null;
    }

    /**
     * Returns all relation names which are part of an outer join.
     */
    public static Set<QualifiedName> outerJoinRelations(List<JoinPair> joinPairs) {
        Set<QualifiedName> outerJoinRelations = new HashSet<>();
        for (JoinPair joinPair : joinPairs) {
            if (joinPair.joinType().isOuter()) {
                outerJoinRelations.add(joinPair.left);
                outerJoinRelations.add(joinPair.right);
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
                                    Function<? super Symbol, Symbol> replaceFunction,
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
            if (joinPair.joinType().isOuter()) {
                if (joinPair.left.equals(name) &&
                    (joinPair.joinType() == JoinType.RIGHT || joinPair.joinType() == JoinType.FULL)) {
                    return true;
                }
                if (joinPair.right.equals(name) &&
                    (joinPair.joinType() == JoinType.LEFT || joinPair.joinType() == JoinType.FULL)) {
                    return true;
                }
            }
        }
        return false;
    }

}
