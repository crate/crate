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

import com.google.common.base.Objects;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.RelationName;
import io.crate.planner.node.dql.join.JoinType;

import javax.annotation.Nullable;

public class JoinPair {

    private final JoinType joinType;

    private final RelationName left;
    private final RelationName right;

    @Nullable
    private final Symbol condition;

    public static JoinPair of(RelationName left, RelationName right, JoinType joinType, Symbol condition) {
        assert condition != null || joinType == JoinType.CROSS : "condition must be present unless it's a cross-join";
        return new JoinPair(left, right, joinType, condition);
    }

    private JoinPair(RelationName left, RelationName right, JoinType joinType, @Nullable Symbol condition) {
        this.left = left;
        this.right = right;
        this.joinType = joinType;
        this.condition = condition;
    }

    public RelationName left() {
        return left;
    }

    public RelationName right() {
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
}
