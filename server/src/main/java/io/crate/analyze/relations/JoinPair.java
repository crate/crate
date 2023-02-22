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

package io.crate.analyze.relations;

import io.crate.expression.symbol.Symbol;
import io.crate.planner.node.dql.join.JoinType;

import javax.annotation.Nullable;
import java.util.Objects;

public class JoinPair {

    private final JoinType joinType;

    private final AnalyzedRelation left;
    private final AnalyzedRelation right;

    @Nullable
    private final Symbol condition;

    public static JoinPair of(AnalyzedRelation left, AnalyzedRelation right, JoinType joinType, Symbol condition) {
        assert condition != null || joinType == JoinType.CROSS : "condition must be present unless it's a cross-join";
        return new JoinPair(left, right, joinType, condition);
    }

    private JoinPair(AnalyzedRelation left, AnalyzedRelation right, JoinType joinType, @Nullable Symbol condition) {
        this.left = left;
        assert right != null;
        this.right = right;
        this.joinType = joinType;
        this.condition = condition;
    }

    public AnalyzedRelation left() {
        return left;
    }

    public AnalyzedRelation right() {
        return right;
    }

    public JoinType joinType() {
        return joinType;
    }

    @Nullable
    public Symbol condition() {
        return condition;
    }

    @Override
    public String toString() {
        return "Join{" + joinType + " " + left + " ⇔ " + right + '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        JoinPair joinPair = (JoinPair) o;
        return joinType == joinPair.joinType &&
               Objects.equals(left, joinPair.left) &&
               Objects.equals(right, joinPair.right) &&
               Objects.equals(condition, joinPair.condition);
    }

    @Override
    public int hashCode() {
        return Objects.hash(joinType, left, right, condition);
    }
}
